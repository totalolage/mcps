import { CurrentPrincipal, type Principal } from "@mcps/core/transport/principal.js";
import { Cache, Data, DateTime, Effect, Exit } from "effect";
import { BoxClients } from "../domain/box-clients.js";
import { type Box, BoxRegistry } from "../domain/boxes.js";
import { isDataBoxId, isMessageId } from "../domain/ids.js";
import type { IsdsClientShape } from "../isds/client.js";
import { IsdsInvalidResponse } from "../isds/errors.js";
import { currentCaller } from "./caller.js";
import { ACCEPTED_DELIVERY_CONSENT_VERSIONS } from "./consent-versions.js";
import { READ_SCOPE } from "./tool-group.js";

type Direction = "received" | "sent";

const uriTemplate = "datovka://messages/{box}{?direction,sender,recipient,subject}";
const subscriptionUri = /^datovka:\/\/messages\/([^/?#\s]+)(?:\?([^#\s]*))?$/u;
const filterNames = new Set(["direction", "sender", "recipient", "subject"]);
const invalidUri = "Use a Datovka message subscription URI with a box label and valid direction, sender, recipient or subject filters.";
const unavailableBox = "Your grant does not cover a data box with that label.";
const certificateRequired = "Received-message subscriptions require a data box configured with a system certificate.";
const lookback = "24 hours";
const sentPageSize = 1000;

const parseUri = Effect.fn("DatovkaMessageEvents.parseUri")(function* (uri: string) {
  const match = subscriptionUri.exec(uri);
  const encodedBox = match?.[1];
  if (encodedBox === undefined || match?.[0] !== uri) return yield* Effect.fail(invalidUri);
  const query = match[2] ?? "";
  const box = yield* Effect.try({
    try: () => {
      // Reject malformed escapes, which URLSearchParams would otherwise replace or preserve.
      decodeURIComponent(query);
      return decodeURIComponent(encodedBox);
    },
    catch: () => invalidUri,
  });
  const params = new URLSearchParams(query);
  const seen = new Set<string>();
  for (const name of params.keys()) {
    if (!filterNames.has(name) || seen.has(name)) return yield* Effect.fail(invalidUri);
    seen.add(name);
  }
  const direction = params.get("direction") ?? "received";
  const sender = params.get("sender");
  const recipient = params.get("recipient");
  const subject = params.get("subject");
  if (
    (direction !== "received" && direction !== "sent") ||
    (sender !== null && !isDataBoxId(sender)) ||
    (recipient !== null && !isDataBoxId(recipient)) ||
    subject === ""
  ) {
    return yield* Effect.fail(invalidUri);
  }
  return { box, direction, sender, recipient, subject: subject?.toLowerCase() } as const;
});

interface RecordMetadata {
  readonly dmID: string;
  readonly dbIDSender?: string | undefined;
  readonly dbIDRecipient?: string | undefined;
  readonly dmDeliveryTime?: DateTime.Utc | undefined;
  readonly dmAnnotation?: string | undefined;
}

const listSentSnapshot = Effect.fn("DatovkaMessageEvents.listSentSnapshot")(function* (
  client: IsdsClientShape,
  from: DateTime.Utc,
  to: DateTime.Utc,
) {
  const records = new Map<string, RecordMetadata>();
  for (let offset = 1; ; offset += sentPageSize) {
    const page = yield* client.listSentMessages({ from, to, offset, limit: sentPageSize });
    for (const record of page) records.set(record.dmID, record);
    if (page.length < sentPageSize) return [...records.values()];
    // The ordinal is the position in the full list, so a repeated page must not loop.
    if ((page.at(-1)?.dmOrdinal ?? 0) < offset + sentPageSize - 1) {
      return yield* new IsdsInvalidResponse({
        service: "info",
        operation: "GetListOfSentMessages",
        detail: "sent-message page did not advance",
      });
    }
  }
});

const eventFor = (direction: Direction, record: RecordMetadata) => {
  const { dmID: id, dbIDSender: sender, dbIDRecipient: recipient, dmDeliveryTime } = record;
  return {
    id,
    direction,
    ...(sender !== undefined && isDataBoxId(sender) ? { sender } : {}),
    ...(recipient !== undefined && isDataBoxId(recipient) ? { recipient } : {}),
    ...(dmDeliveryTime === undefined ? {} : { deliveredAt: DateTime.formatIso(dmDeliveryTime) }),
  };
};

class SnapshotKey extends Data.Class<{
  readonly grantId: string;
  readonly boxId: string;
  readonly direction: Direction;
  readonly updatedAt: number;
}> {}

const checkAuthKind = (box: Box, direction: Direction) =>
  direction === "received" && box.authKind !== "certificate" ? Effect.fail(certificateRequired) : Effect.void;

export const makeMessageEvents = Effect.gen(function* () {
  const boxes = yield* BoxRegistry;
  const clients = yield* BoxClients;
  const getBox = (boxId: string) => boxes.get(boxId).pipe(Effect.mapError(() => unavailableBox));

  const loadSnapshot = Effect.fn("DatovkaMessageEvents.loadSnapshot")(function* (key: SnapshotKey) {
    const { boxId, direction } = key;
    return yield* clients
      .use(
        boxId,
        Effect.fnUntraced(function* (client) {
          // Check again under the client lock in case an administrator replaced the credentials.
          const box = yield* getBox(boxId);
          yield* checkAuthKind(box, direction);
          const now = yield* DateTime.now;
          const from = DateTime.subtractDuration(now, lookback);
          if (direction === "sent") return yield* listSentSnapshot(client, from, now);
          const records = yield* client.listReceivedNotifications(from);
          return records.filter((record) => record.dbIDRecipient === box.dbId);
        }),
      )
      .pipe(
        Effect.mapError((error) => {
          if (typeof error === "string") return error;
          if (error._tag === "IsdsStatusError" && error.operation === "GetListForNotifications" && error.code === "1273") {
            return "This data box must be registered in ISDS for received-message notifications.";
          }
          return "Could not read Datovka message events from ISDS.";
        }),
      );
  });

  const snapshots = yield* Cache.makeWith(loadSnapshot, {
    capacity: 128,
    timeToLive: (exit) => (Exit.isSuccess(exit) ? "45 seconds" : 0),
  });

  const open = Effect.fn("DatovkaMessageEvents.open")(function* (uri: string, principal: Principal) {
    const filters = yield* parseUri(uri);
    const caller = yield* currentCaller.pipe(
      Effect.provideService(CurrentPrincipal, principal),
      Effect.mapError(() => "Datovka message subscriptions require an OAuth grant."),
    );
    if (!caller.scopes.has(READ_SCOPE)) return yield* Effect.fail("Your grant lacks the isds:read scope.");
    const consent = caller.deliveryConsentVersion;
    if (consent === null || !ACCEPTED_DELIVERY_CONSENT_VERSIONS.has(consent)) {
      return yield* Effect.fail("Authorize the client again with an accepted Datovka read consent.");
    }
    const box = (yield* boxes.list).find((candidate) => candidate.label === filters.box && caller.boxIds.has(candidate.id));
    if (box === undefined) return yield* Effect.fail(unavailableBox);

    const { direction, sender, recipient, subject } = filters;
    yield* checkAuthKind(box, direction);
    const source = JSON.stringify([caller.grantId, box.id, direction]);
    const read = Effect.gen(function* () {
      const current = yield* getBox(box.id);
      yield* checkAuthKind(current, direction);
      const key = new SnapshotKey({ grantId: caller.grantId, boxId: current.id, direction, updatedAt: current.updatedAt });
      const records = yield* Cache.get(snapshots, key);
      return records
        .filter(
          (record) =>
            isMessageId(record.dmID) &&
            (sender === null || record.dbIDSender === sender) &&
            (recipient === null || record.dbIDRecipient === recipient) &&
            (subject === undefined || record.dmAnnotation?.toLowerCase().includes(subject) === true),
        )
        .map((record) => eventFor(direction, record));
    });
    return { source, read };
  });

  return {
    name: "datovka_messages",
    description:
      "Watch messages in a granted data box by label. Direction defaults to received. Filter by exact sender or recipient data box ID and case-insensitive subject substring. Received notifications require a registered system-certificate box.",
    uriTemplate,
    open,
  };
});
