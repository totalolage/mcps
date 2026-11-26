import { assert, describe, it } from "@effect/vitest";
import { layerStdio } from "@mcps/core/transport/stdio.js";
import { Deferred, Effect, Fiber, Layer, Queue, Ref, Sink, Stdio, Stream } from "effect";
import { TestClock } from "effect/testing";
import { SERVER_INFO, ToolsLive } from "../src/app.js";
import type { FioApiShape } from "../src/fio-api.js";
import { makeCacheToolkit } from "../src/mcp/cache-tools.js";
import { makeFioToolkit } from "../src/mcp/fio-tools.js";
import { makePaymentEvents } from "../src/mcp/payment-events.js";
import { normalizeTransaction, type Transaction } from "../src/statement.js";
import { STATEMENT } from "./support/fio-api.js";
import { ACCOUNT_NAMES, FIO_TOKEN, testConfig, testServices } from "./support/transport.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** The stdio app on in-memory stdin/stdout; returns a request function and every stdout chunk. */
const startStdio = Effect.fnUntraced(function* (fio: Partial<FioApiShape> = {}) {
  const stdin = yield* Queue.unbounded<Uint8Array>();
  const stdout = yield* Queue.unbounded<string>();
  const written: Array<string> = [];
  const release = yield* Deferred.make<void>();
  const record = (chunk: string | Uint8Array) => {
    const text = typeof chunk === "string" ? chunk : decoder.decode(chunk);
    written.push(text);
    return Queue.offer(stdout, text);
  };
  const platformStdio = Stdio.layerTest({
    stdin: Stream.fromQueue(stdin),
    stdout: () => Sink.forEach(record),
  });
  // Launched on its own fiber: the stdio protocol interrupts the fiber that
  // built it once stdout closes, as a process would exit.
  const server = yield* Effect.forkScoped(
    Layer.launch(
      ToolsLive.pipe(
        Layer.provide(Layer.unwrap(Effect.map(makePaymentEvents(), (events) => layerStdio(SERVER_INFO, events)))),
        Layer.provide(platformStdio),
        Layer.provide(testServices(testConfig(), release, undefined, fio)),
      ),
    ),
  );
  const send = (message: object) => Queue.offer(stdin, encoder.encode(`${JSON.stringify(message)}\n`));
  // A server that fails to start fails the test instead of hanging it.
  const receive = Effect.map(Effect.raceFirst(Queue.take(stdout), Fiber.join(server)), (chunk) => JSON.parse(chunk));
  const request = (message: object) => send(message).pipe(Effect.andThen(receive));
  return { send, receive, request, written };
});

describe("stdio transport", () => {
  it.live("speaks MCP on stdout, one JSON-RPC frame per write, with secrets redacted", () =>
    Effect.gen(function* () {
      const { send, request, written } = yield* startStdio();

      const init = yield* request({
        jsonrpc: "2.0",
        id: 1,
        method: "initialize",
        params: { protocolVersion: "2025-11-25", capabilities: {}, clientInfo: { name: "t", version: "0" } },
      });
      assert.strictEqual(init.result.serverInfo.name, SERVER_INFO.name);
      yield* send({ jsonrpc: "2.0", method: "notifications/initialized" });

      const list = yield* request({ jsonrpc: "2.0", id: 2, method: "tools/list", params: {} });
      assert.deepStrictEqual(
        list.result.tools.map((t: { name: string }) => t.name).sort(),
        [
          ...Object.keys(makeFioToolkit(ACCOUNT_NAMES).tools),
          ...Object.keys(makeCacheToolkit(ACCOUNT_NAMES).tools),
        ].sort(),
      );

      // McpServer echoes the unknown tool name in its own error text.
      const unknown = yield* request({
        jsonrpc: "2.0",
        id: 3,
        method: "tools/call",
        params: { name: FIO_TOKEN, arguments: {} },
      });
      assert.strictEqual(unknown.id, 3);
      assert.strictEqual(unknown.error.message, "Tool '***' not found");

      for (const chunk of written) {
        assert.notInclude(chunk, FIO_TOKEN);
        assert.isTrue(chunk.endsWith("\n"), "each write is one newline-terminated frame");
        const frame = JSON.parse(chunk);
        assert.strictEqual(frame.jsonrpc, "2.0");
      }
      assert.strictEqual(written.length, 3);
    }).pipe(Effect.scoped),
  );

  it.effect("delivers booked payment updates to a modern stdio client until it cancels the listen request", () =>
    Effect.gen(function* () {
      const movement = (id: number, amount: number): Transaction => ({ ...normalizeTransaction({}), id, amount, currency: "CZK" });
      const transactions = yield* Ref.make([movement(1, 100)]);
      const reads = yield* Ref.make(0);
      const { send, receive, request, written } = yield* startStdio({
        periods: Effect.fnUntraced(function* (_account, _from, _to, priority) {
          assert.strictEqual(priority, "background");
          yield* Ref.update(reads, (count) => count + 1);
          const rows = yield* Ref.get(transactions);
          return { ...STATEMENT, transactionCount: rows.length, transactions: rows };
        }),
      });
      const _meta = {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
      };
      const uri = "fio://payments/main?direction=in&minAmount=50";
      const ack = yield* request({
        jsonrpc: "2.0",
        id: 12,
        method: "subscriptions/listen",
        params: { _meta, notifications: { resourceSubscriptions: [uri] } },
      });
      assert.deepInclude(ack, {
        method: "notifications/subscriptions/acknowledged",
        params: { _meta: { "io.modelcontextprotocol/subscriptionId": 12 }, notifications: { resourceSubscriptions: [uri] } },
      });
      assert.strictEqual(written.length, 1);
      yield* Ref.update(transactions, (rows) => [...rows, movement(2, -300), movement(3, 25), movement(4, 75)]);
      yield* TestClock.adjust("1 minute");
      const update = yield* receive;
      assert.deepInclude(update, {
        method: "notifications/resources/updated",
        params: { _meta: { "io.modelcontextprotocol/subscriptionId": 12 }, uri },
      });
      const read = yield* request({ jsonrpc: "2.0", id: 13, method: "resources/read", params: { _meta, uri } });
      assert.deepStrictEqual(JSON.parse(read.result.contents[0].text).map((row: { id: string }) => row.id), ["1", "4"]);

      yield* TestClock.adjust("1 minute");
      assert.strictEqual(written.length, 3);
      yield* send({ jsonrpc: "2.0", method: "notifications/cancelled", params: { _meta, requestId: 12 } });
      // A subsequent request confirms that stdio consumed the cancellation.
      const discovery = yield* request({ jsonrpc: "2.0", id: 14, method: "server/discover", params: { _meta } });
      assert.strictEqual(discovery.id, 14);
      const before = yield* Ref.get(reads);
      yield* TestClock.adjust("1 minute");
      assert.strictEqual(yield* Ref.get(reads), before);
    }).pipe(Effect.scoped),
  );
});
