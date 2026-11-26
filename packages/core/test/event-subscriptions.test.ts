import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { JSONRPCMessage } from "@modelcontextprotocol/sdk/types.js";
import { Effect, Layer, Schema, Stream } from "effect";
import { McpProtocol, McpServer, Tool, Toolkit } from "effect/ai";
import { HttpRouter, HttpServer } from "effect/http";
import { expect, it, vi } from "vitest";
import type { EventResource } from "../src/mcp/event-subscriptions.js";
import { ToolPolicy } from "../src/mcp/grants.js";
import { Redactor } from "../src/redaction.js";
import { Resources } from "../src/resources.js";
import { Auth, Unauthorized } from "../src/transport/auth.js";
import { httpRouterApp } from "../src/transport/http.js";

const version = "2026-07-28";
const _meta = { "io.modelcontextprotocol/protocolVersion": version, "io.modelcontextprotocol/clientCapabilities": {} };
const subscriptionId = "io.modelcontextprotocol/subscriptionId";
const url = new URL("http://mcp.example.test/mcp");
const template = "demo://events/{box}{?direction}";
const incoming = "demo://events/inbox?direction=in";
const outgoing = "demo://events/inbox?direction=out";
const longLabel = `demo://events/${encodeURIComponent("📬".repeat(64))}?direction=in`;

const statusToolkit = Toolkit.make(Tool.make("status", { parameters: Schema.Struct({ label: Schema.String }), success: Schema.String }));
const tools = McpServer.toolkit(statusToolkit).pipe(
  Layer.provide(statusToolkit.toLayer({ status: ({ label }) => Effect.succeed(label) })),
);

function start() {
  const rows = [{ id: "baseline", direction: "in" }];
  let reads = 0;
  let revoked = false;
  let source = "inbox-id";
  let unavailable = false;
  let blocked = false;
  let interrupted = false;
  const events: EventResource = {
    uriTemplate: template,
    name: "Demo events",
    description: "Recent events",
    open: (uri, principal) => {
      if (uri !== incoming && uri !== outgoing && uri !== longLabel) return Effect.fail("Unknown event resource.");
      if (principal.resources !== "all" && !principal.resources.includes(source)) return Effect.fail("No access to the event resource.");
      return Effect.succeed({
        source,
        read: Effect.suspend(() => {
          reads++;
          if (blocked) {
            return Effect.never.pipe(Effect.ensuring(Effect.sync(() => { interrupted = true; })));
          }
          return unavailable ? Effect.fail("Unavailable.") : Effect.succeed(rows.filter((row) => uri.endsWith(row.direction)));
        }),
      });
    },
  };
  const auth = Layer.succeed(
    Auth,
    Auth.of({
      authenticate: (request) => {
        const subject = request.headers.authorization;
        if (revoked || (subject !== "Bearer owner" && subject !== "Bearer other")) {
          return Effect.fail(new Unauthorized({ challenge: "Bearer" }));
        }
        const owner = subject === "Bearer owner";
        return Effect.succeed({ subject, scopes: owner ? ["status:read"] : [], resources: owner ? "all" : [] });
      },
    }),
  );
  const web = HttpRouter.toWebHandler(
    httpRouterApp(
      tools,
      auth,
      { name: "events-demo", version: "0.0.0", instructions: "", protocols: [McpProtocol.v2025_11_25] },
      { host: "127.0.0.1", port: 0, allowedOrigins: [], allowedHosts: [], trustedProxies: [] },
      events,
    ).pipe(
      Layer.provide(Layer.succeed(Resources, Resources.of({ list: Effect.succeed([]), changes: Stream.empty }))),
      Layer.provide(ToolPolicy.layer({ scopes: () => ["status:read"], resourceless: () => true, resourceParameter: "box" })),
      Layer.provide(Redactor.layer(Effect.succeed([]))),
      Layer.provide(HttpServer.layerServices),
    ),
    { disableLogger: true },
  );
  const clients: StreamableHTTPClientTransport[] = [];
  const transport = (subject: string) => {
    const connection = new StreamableHTTPClientTransport(url, {
      requestInit: { headers: { authorization: `Bearer ${subject}` } },
      fetch: (url, init) => {
        const headers = new Headers(init?.headers);
        if (typeof init?.body === "string") {
          const request = JSON.parse(init.body);
          headers.set("mcp-method", request.method);
          if (request.method === "resources/read") headers.set("mcp-name", request.params.uri);
        }
        return web.handler(new Request(url, { ...init, headers }));
      },
    });
    clients.push(connection);
    return connection;
  };
  return {
    add: (id: string, direction: string) => rows.push({ id, direction }),
    reads: () => reads,
    revoke: () => {
      revoked = true;
    },
    replaceSource: () => {
      source = "replacement-id";
    },
    unavailable: (value: boolean) => {
      unavailable = value;
    },
    block: () => {
      blocked = true;
    },
    interrupted: () => interrupted,
    transport,
    async connect(subject: string) {
      const connection = transport(subject);
      const messages: JSONRPCMessage[] = [];
      connection.onmessage = (message) => messages.push(message);
      connection.setProtocolVersion(version);
      await connection.start();
      return {
        messages,
        close: () => connection.close(),
        request: (id: number, method: string, params: Record<string, unknown> = {}) =>
          connection.send({ jsonrpc: "2.0", id, method, params: { _meta, ...params } }),
      };
    },
    async close() {
      await Promise.all(clients.map((connection) => connection.close()));
      await web.dispose();
    },
  };
}

it("uses one POST event stream per subscription, keeps its baseline silent and reads the filtered resource", async () => {
  const server = start();
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    const stream = await server.connect("owner");
    await stream.request(1, "subscriptions/listen", { notifications: { resourceSubscriptions: [incoming, outgoing, incoming] } });
    const second = await server.connect("owner");
    await second.request(7, "subscriptions/listen", { notifications: { resourceSubscriptions: [incoming] } });
    await vi.waitFor(() =>
      expect(stream.messages).toEqual([
        expect.objectContaining({
          method: "notifications/subscriptions/acknowledged",
          params: { _meta: { [subscriptionId]: 1 }, notifications: { resourceSubscriptions: [incoming, outgoing] } },
        }),
      ]),
    );
    await vi.waitFor(() => expect(second.messages[0]).toMatchObject({ method: "notifications/subscriptions/acknowledged" }));

    server.add("match", "in");
    await vi.advanceTimersByTimeAsync(60_000);
    expect(stream.messages).toHaveLength(2);
    expect(stream.messages[1]).toMatchObject({
      method: "notifications/resources/updated",
      params: { _meta: { [subscriptionId]: 1 }, uri: incoming },
    });
    expect(second.messages).toHaveLength(2);
    expect(second.messages[1]).toMatchObject({
      method: "notifications/resources/updated",
      params: { _meta: { [subscriptionId]: 7 }, uri: incoming },
    });

    const reader = await server.connect("owner");
    await reader.request(2, "resources/templates/list");
    expect(reader.messages[0]).toMatchObject({ result: { resourceTemplates: [{ uriTemplate: template }] } });
    await reader.request(3, "resources/read", { uri: incoming });
    expect(reader.messages[1]).toMatchObject({
      result: { contents: [{ uri: incoming, text: JSON.stringify([{ id: "baseline", direction: "in" }, { id: "match", direction: "in" }]) }] },
    });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(stream.messages).toHaveLength(2);

    await stream.close();
    await second.close();
    const reads = server.reads();
    server.add("after-cancel", "in");
    await vi.advanceTimersByTimeAsync(60_000);
    expect(server.reads()).toBe(reads);
    expect(stream.messages).toHaveLength(2);
  } finally {
    vi.useRealTimers();
    await server.close();
  }
});

it("checks grants before subscribing or reading and closes streams when authorization ends", async () => {
  const server = start();
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    const other = await server.connect("other");
    await other.request(4, "subscriptions/listen", { notifications: { resourceSubscriptions: [incoming] } });
    await vi.waitFor(() => expect(other.messages[0]).toMatchObject({ id: 4, error: { code: -32602 } }));
    await other.request(5, "resources/read", { uri: incoming });
    expect(other.messages[1]).toMatchObject({ id: 5, error: expect.any(Object) });
    expect(server.reads()).toBe(0);

    const owner = await server.connect("owner");
    await owner.request(6, "subscriptions/listen", { notifications: { resourceSubscriptions: [incoming] } });
    await vi.waitFor(() => expect(owner.messages[0]).toMatchObject({ method: "notifications/subscriptions/acknowledged" }));
    server.add("hidden", "in");
    server.revoke();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(owner.messages[1]).toMatchObject({ id: 6, result: { _meta: { [subscriptionId]: 6 }, resultType: "complete" } });
    expect(server.reads()).toBe(1);
  } finally {
    vi.useRealTimers();
    await server.close();
  }
});

it("keeps a successful baseline across source errors and stops if the label resolves to a different source", async () => {
  const server = start();
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    const owner = await server.connect("owner");
    await owner.request(7, "subscriptions/listen", { notifications: { resourceSubscriptions: [incoming] } });
    await vi.waitFor(() => expect(owner.messages[0]).toMatchObject({ method: "notifications/subscriptions/acknowledged" }));
    server.unavailable(true);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(owner.messages).toHaveLength(1);
    server.unavailable(false);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(owner.messages).toHaveLength(1);
    server.replaceSource();
    const reads = server.reads();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(owner.messages[1]).toMatchObject({ id: 7, result: { resultType: "complete" } });
    expect(server.reads()).toBe(reads);
  } finally {
    vi.useRealTimers();
    await server.close();
  }
});

it("interrupts an outstanding provider poll when a grant is revoked", async () => {
  const server = start();
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    const owner = await server.connect("owner");
    await owner.request(8, "subscriptions/listen", { notifications: { resourceSubscriptions: [incoming] } });
    await vi.waitFor(() => expect(owner.messages[0]).toMatchObject({ method: "notifications/subscriptions/acknowledged" }));
    server.block();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(server.reads()).toBe(2);
    expect(server.interrupted()).toBe(false);
    server.revoke();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(owner.messages[1]).toMatchObject({ id: 8, result: { resultType: "complete" } });
    expect(server.interrupted()).toBe(true);
  } finally {
    vi.useRealTimers();
    await server.close();
  }
});

it("lets the client read an acknowledged URI with the longest Unicode label", async () => {
  const server = start();
  try {
    const owner = await server.connect("owner");
    await owner.request(9, "subscriptions/listen", { notifications: { resourceSubscriptions: [longLabel] } });
    await vi.waitFor(() => expect(owner.messages[0]).toMatchObject({
      method: "notifications/subscriptions/acknowledged",
      params: { notifications: { resourceSubscriptions: [longLabel] } },
    }));
    await owner.request(10, "resources/read", { uri: longLabel });
    expect(owner.messages[1]).toMatchObject({ id: 10, result: { contents: [{ uri: longLabel, text: expect.stringContaining("baseline") }] } });
  } finally {
    await server.close();
  }
});

it("still initializes a legacy SDK client and advertises modern subscriptions on explicit discovery", async () => {
  const server = start();
  try {
    const old = new Client({ name: "legacy", version: "1.0.0" });
    await old.connect(server.transport("owner"));
    expect(old.getServerVersion()?.name).toBe("events-demo");
    expect(old.getServerCapabilities()?.resources?.subscribe).toBe(false);
    expect((await old.listResourceTemplates()).resourceTemplates).toEqual([]);
    expect((await old.listTools()).tools.map(({ name }) => name)).toEqual(["status"]);
    const modern = await server.connect("owner");
    await modern.request(8, "server/discover");
    expect(modern.messages[0]).toMatchObject({ result: { capabilities: { resources: { subscribe: true } } } });
    await modern.request(9, "tools/list");
    expect(modern.messages[1]).toMatchObject({ result: { tools: [{ name: "status" }] } });
    const other = await server.connect("other");
    await other.request(10, "tools/list");
    expect(other.messages[0]).toMatchObject({ result: { tools: [] } });
  } finally {
    await server.close();
  }
});
