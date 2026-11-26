import { Clock, Context, Deferred, Effect, Layer, Result, type Scope } from "effect";
import { McpProtocol, McpSchema, McpServer } from "effect/ai";
import { CurrentPrincipal, type Principal } from "../transport/principal.js";
import type { ServerInfo } from "../transport/server-info.js";

const version = McpProtocol.v2026_07_28.protocolVersion;
const subscriptionId = "io.modelcontextprotocol/subscriptionId";
const pollInterval = "1 minute";
const forgetAfterMs = 7 * 24 * 60 * 60 * 1_000;
const maxUris = 8;
const maxUriLength = 2_048;

interface EventRow {
  readonly id: string;
}

interface EventSnapshot {
  /** The immutable grant-bound source identity. Never sent to the client. */
  readonly source: string;
  readonly read: Effect.Effect<ReadonlyArray<EventRow>, string>;
}

export interface EventResource {
  readonly name: string;
  readonly description: string;
  /** A single label segment, followed by optional RFC 6570 query variables. */
  readonly uriTemplate: string;
  /** Validate the URI and current grant before opening the source. Errors are safe for clients. */
  readonly open: (uri: string, principal: Principal) => Effect.Effect<EventSnapshot, string>;
}

/** A stream can outlive the token used to open it. HTTP supplies a fresh auth check. */
export const RecheckEventPrincipal = Context.Reference<Effect.Effect<Principal, string> | undefined>(
  "mcps/mcp/RecheckEventPrincipal",
  { defaultValue: () => undefined },
);

const activeSubscription = Context.Reference<{
  readonly acknowledged: Deferred.Deferred<void>;
  readonly authorized: Effect.Effect<Principal, string>;
} | undefined>("mcps/mcp/ActiveEventSubscription", { defaultValue: () => undefined });

const mimeType = "application/json";

/** The same grant and filters apply when reading the resource after an update. */
export const eventResourceLayer = (events: EventResource, local?: Principal) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const server = yield* McpServer.McpServer;
      const path = events.uriTemplate.replace(/\{\?[^}]+\}$/, "");
      yield* server.addResourceTemplate({
        template: McpSchema.ResourceTemplate.make({
          uriTemplate: events.uriTemplate,
          name: events.name,
          description: events.description,
          mimeType,
        }),
        annotations: Context.make(McpSchema.EnabledWhen, (client) => client.protocolVersion === version),
        // Effect's resource matcher routes the path. The provider validates
        // the original URI, including the query and percent-encoded label.
        routerPath: path.replace(":", "::").replace(/\{[^{}]+\}/, ":0"),
        completions: {},
        handle: Effect.fnUntraced(function* (uri: string) {
          const principal = local ?? (yield* CurrentPrincipal);
          const snapshot = yield* events.open(uri, principal).pipe(Effect.mapError((message) => new McpSchema.InvalidParams({ message })));
          const rows = yield* snapshot.read.pipe(Effect.mapError((message) => new McpSchema.InternalError({ message })));
          return McpSchema.ReadResourceResult.make({
            contents: [McpSchema.TextResourceContents.make({ uri, mimeType, text: JSON.stringify(rows) })],
          });
        }),
      });
    }),
  );

interface ListenRequest {
  readonly notifications: {
    readonly resourceSubscriptions?: ReadonlyArray<string> | undefined;
  };
}

interface ListenContext {
  readonly client: { readonly id: number };
  readonly requestId: string | number;
}

interface ListenError {
  readonly code: number;
  readonly message: string;
}

type Listen = (request: ListenRequest, context: ListenContext) => Effect.Effect<unknown, ListenError, Scope.Scope>;
type Send = (version: string, clientId: number, notification: McpProtocol.ProjectedNotification) => Effect.Effect<void>;

interface ListenTarget {
  readonly context: { readonly sendNotification?: Send | undefined };
  readonly install: (protocol: unknown, rpcs: unknown, handlers: Record<string, unknown>) => Effect.Effect<void>;
}

interface WatchedResource {
  readonly uri: string;
  readonly source: string;
  readonly seen: Map<string, number>;
}

function hasNewRows(seen: Map<string, number>, rows: ReadonlyArray<EventRow>, now: number) {
  let changed = false;
  for (const { id } of rows) {
    if (!seen.has(id)) changed = true;
    seen.set(id, now);
  }
  for (const [id, lastSeen] of seen) {
    if (now - lastSeen > forgetAfterMs) seen.delete(id);
  }
  return changed;
}

const listenForEvents = (nativeListen: Listen, send: Send, events: EventResource, info: ServerInfo, local?: Principal): Listen =>
  Effect.fnUntraced(function* (request: ListenRequest, context: ListenContext) {
    const requested = request.notifications.resourceSubscriptions ?? [];
    if (requested.length > maxUris || requested.some((uri) => uri.length > maxUriLength)) {
      return yield* Effect.fail({ code: -32602, message: "A subscription accepts up to 8 resource URIs of up to 2048 characters each." });
    }
    const uris = Array.from(new Set(requested));
    const principal = local ?? (yield* CurrentPrincipal);
    const recheck =
      local === undefined
        ? (yield* RecheckEventPrincipal) ?? Effect.fail("The subscription authorization is unavailable.")
        : Effect.succeed(local);
    const authorized = recheck.pipe(
      Effect.filterOrFail((current) => current.subject === principal.subject, () => "The subscription authorization changed."),
    );
    const initial = yield* Effect.forEach(uris, (uri) =>
      Effect.map(events.open(uri, principal), (snapshot) => ({ uri, snapshot })),
    ).pipe(
      Effect.mapError((message) => ({ code: -32602, message })),
    );
    const now = yield* Clock.currentTimeMillis;
    const watched = yield* Effect.forEach(
      initial,
      ({ uri, snapshot }): Effect.Effect<WatchedResource, string> =>
        Effect.map(snapshot.read, (rows) => ({
          uri,
          source: snapshot.source,
          seen: new Map(rows.map(({ id }) => [id, now])),
        })),
      { concurrency: maxUris },
    ).pipe(
      Effect.mapError((message) => ({ code: -32603, message })),
    );
    const resolve = Effect.fnUntraced(function* (current: Principal, resource: WatchedResource) {
      const snapshot = yield* events.open(resource.uri, current);
      if (snapshot.source !== resource.source) return yield* Effect.fail("The subscription source changed.");
      return snapshot;
    });
    const resolveAll = Effect.flatMap(authorized, (current) =>
      Effect.forEach(watched, (resource) => Effect.map(resolve(current, resource), (snapshot) => ({ resource, snapshot }))),
    );
    yield* resolveAll.pipe(Effect.mapError((message) => ({ code: -32602, message })));

    const acknowledged = yield* Deferred.make<void>();
    const poll = Effect.gen(function* () {
      while (true) {
        yield* Effect.sleep(pollInterval);
        const sources = yield* resolveAll;
        const results = yield* Effect.forEach(
          sources,
          ({ resource, snapshot }) => Effect.map(Effect.result(snapshot.read), (rows) => ({ resource, rows })),
          { concurrency: maxUris },
        );
        const checkedAt = yield* Clock.currentTimeMillis;
        for (const { resource, rows } of results) {
          yield* resolve(yield* authorized, resource);
          if (Result.isFailure(rows) || !hasNewRows(resource.seen, rows.success, checkedAt)) continue;
          yield* send(version, context.client.id, {
            tag: "notifications/resources/updated",
            payload: { _meta: { [subscriptionId]: context.requestId }, uri: resource.uri },
          });
        }
      }
    }).pipe(
      Effect.orElseSucceed(() => ({
        resultType: "complete",
        _meta: { [subscriptionId]: context.requestId, "io.modelcontextprotocol/serverInfo": { name: info.name, version: info.version } },
      })),
    );
    const notifications =
      request.notifications.resourceSubscriptions === undefined
        ? request.notifications
        : { ...request.notifications, resourceSubscriptions: uris };
    return yield* Effect.raceFirst(
      nativeListen({ ...request, notifications }, context).pipe(
        Effect.provideService(activeSubscription, { acknowledged, authorized }),
      ),
      Deferred.await(acknowledged).pipe(Effect.andThen(poll)),
    );
  });

/**
 * Effect owns the POST stream, acknowledgment, cancellation and ordinary list
 * notifications. Its default handler accepts arbitrary URIs and takes no
 * resource lifecycle hook. Interpose at adapter installation so each stream
 * checks its own URIs and sends updates only on that stream.
 */
export function eventProtocols(info: ServerInfo, events: EventResource, local?: Principal): ServerInfo["protocols"] {
  const adapter = McpProtocol.v2026_07_28;
  const install = (adapter as McpProtocol.AnyProtocolAdapter<string, never>).installHandlers;
  return [
    ...info.protocols,
    {
      ...adapter,
      installHandlers: (core, lifecycle, target: ListenTarget) => {
        const send = target.context.sendNotification;
        if (send === undefined) return install(core, lifecycle, target);
        return install(core, lifecycle, {
          ...target,
          context: {
            ...target.context,
            sendNotification: Effect.fnUntraced(function* (
              revision: string,
              clientId: number,
              notification: McpProtocol.ProjectedNotification,
            ) {
              const active = yield* activeSubscription;
              const sendMessage = send(revision, clientId, notification);
              if (active === undefined) return yield* sendMessage;
              if (notification.tag === "notifications/subscriptions/acknowledged") {
                yield* sendMessage;
                yield* Deferred.succeed(active.acknowledged, undefined);
              } else if (Result.isSuccess(yield* Effect.result(active.authorized))) {
                yield* sendMessage;
              }
            }),
          },
          install: (protocol: unknown, rpcs: unknown, handlers: Record<string, unknown>) => {
            const listen = handlers["subscriptions/listen"];
            if (typeof listen !== "function") return Effect.die("Effect did not install MCP subscriptions/listen");
            return target.install(protocol, rpcs, {
              ...handlers,
              "subscriptions/listen": listenForEvents(listen as Listen, send, events, info, local),
            });
          },
        });
      },
    },
  ];
}
