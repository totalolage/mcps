import { assert, describe, it } from "@effect/vitest";
import type { Principal } from "@mcps/core/transport/principal.js";
import { Effect, Layer } from "effect";
import { TestClock } from "effect/testing";
import { BoxClients } from "../../src/domain/box-clients.js";
import { BoxLocks } from "../../src/domain/box-locks.js";
import type { BoxAuthKind } from "../../src/domain/boxes.js";
import { DomainConfig } from "../../src/domain/config.js";
import { makeMessageEvents } from "../../src/mcp/message-events.js";
import { READ_SCOPE } from "../../src/mcp/tool-group.js";
import { DELIVERY_CONSENT_VERSION } from "../../src/oauth/consent.js";
import { fakeBox, makeFakeBoxRegistry } from "../domain/support/fake-box-registry.js";
import { certificateCredentials, type FakeRespond, fakeIsdsTransports, passwordCredentials } from "../domain/support/fake-isds.js";
import { dmStatus, ok, soapResponse } from "../isds/fake-transport.js";

const boxId = "box_AAAAAAAAAAAAAAAAAAAAAA";
const recipientId = "abc2def";
const boxLabel = "Podání / Praha";
const uri = `datovka://messages/${encodeURIComponent(boxLabel)}`;
const principal: Principal = {
  subject: "grant-1",
  clientId: "client-1",
  scopes: [READ_SCOPE],
  resources: [boxId],
  settings: { deliveryConsentVersion: DELIVERY_CONSENT_VERSION },
};

const notificationOperation = "GetListForNotifications";
const sentOperation = "GetListOfSentMessages";
const now = "2026-11-26T10:00:00.000Z";
const notifiedAt = "2026-11-26T09:10:00.000Z";

const notification = (
  id: string,
  sender = "xyz9876",
  time = notifiedAt,
  recipient = recipientId,
  type = 1,
) =>
  `<ntfRecord><ntfType>${type}</ntfType><dmID>${id}</dmID><dmPersonalDelivery>0</dmPersonalDelivery>` +
  `<dmDeliveryTime>${time}</dmDeliveryTime><dbIDRecipient>${recipient}</dbIDRecipient>` +
  `<dmAnnotation>Faktura &amp; příloha</dmAnnotation><dbIDSender>${sender}</dbIDSender>` +
  "<dmSender>Untrusted sender name</dmSender></ntfRecord>";

const notifications = (records: string, continues = false) =>
  ok(
    soapResponse(
      notificationOperation,
      `<ntfRecords>${records}</ntfRecords><ntfListContinues>${continues}</ntfListContinues>${dmStatus("0000")}`,
    ),
  );

const sentRecord = (ordinal: number) =>
  `<dmRecord><dmOrdinal>${ordinal}</dmOrdinal><dmID>${ordinal}</dmID><dbIDSender>${recipientId}</dbIDSender>` +
  "<dmSenderType>10</dmSenderType><dbIDRecipient>xyz9876</dbIDRecipient>" +
  `<dmAnnotation>Faktura &amp; příloha</dmAnnotation><dmMessageStatus>4</dmMessageStatus>` +
  `<dmDeliveryTime>${notifiedAt}</dmDeliveryTime></dmRecord>`;

const sentRecords = (records: string) =>
  ok(soapResponse(sentOperation, `<dmRecords>${records}</dmRecords>${dmStatus("0000")}`));

const setup = Effect.fn("setup")(function* (respond: FakeRespond, authKind: BoxAuthKind = "certificate") {
  yield* TestClock.setTime(Date.parse(now));
  const registry = yield* makeFakeBoxRegistry([
    {
      box: { ...fakeBox(boxId, recipientId), label: boxLabel, authKind },
      credentials: authKind === "certificate" ? certificateCredentials("test-certificate") : passwordCredentials("test-user"),
    },
  ]);
  const transport = fakeIsdsTransports(respond);
  const clients = BoxClients.Default.pipe(
    Layer.provide(Layer.mergeAll(registry.layer, transport.layer, BoxLocks.Default, DomainConfig.layer({ dataDir: "/unused" }))),
  );
  const services = yield* Layer.build(Layer.merge(registry.layer, clients));
  const events = yield* makeMessageEvents.pipe(Effect.provide(services));
  return { events, registry, requests: transport.requests };
});

describe("Datovka message subscriptions", () => {
  it.effect("pages the read-only received feed and shares each snapshot across URI filters", () =>
    Effect.gen(function* () {
      const later = "2026-11-26T09:12:00.000Z";
      const first = notifications(notification("101"), true);
      const second = notifications(
        notification("102", "pqr1234", later) +
          notification("103", "xyz9876", later, "qrs6789") +
          notification("104", "xyz9876", later, recipientId, 2),
      );
      const { events, requests } = yield* setup((_config, request) =>
        Effect.succeed(request.envelope.includes(notifiedAt) ? second : first),
      );
      const matching = yield* events.open(`${uri}?sender=xyz9876&recipient=${recipientId}&subject=KTURA%20%26`, principal);
      const all = yield* events.open(uri, principal);
      assert.strictEqual(matching.source, all.source);
      assert.isEmpty(requests);

      const [filtered, rows] = yield* Effect.all([matching.read, all.read], { concurrency: "unbounded" });
      assert.deepStrictEqual(filtered, [{
        id: "101",
        direction: "received",
        sender: "xyz9876",
        recipient: recipientId,
        deliveredAt: notifiedAt,
      }]);
      assert.deepStrictEqual(rows.map(({ id }) => id), ["101", "102"]);
      assert.deepStrictEqual(requests.map(({ request }) => request.operation), [notificationOperation, notificationOperation]);
      assert.include(requests[0]?.request.envelope, "<ntfFromTime>2026-11-25T10:00:00.000Z</ntfFromTime>");
      assert.include(requests[0]?.request.envelope, "<ntfScope>SINGLE_BOX</ntfScope>");
      assert.include(requests[1]?.request.envelope, `<ntfFromTime>${notifiedAt}</ntfFromTime>`);

      const otherGrant = yield* events.open(uri, { ...principal, subject: "grant-2" });
      assert.notStrictEqual(otherGrant.source, all.source);
      yield* otherGrant.read;
      assert.lengthOf(requests, 4);
      yield* TestClock.adjust("45 seconds");
      yield* all.read;
      assert.lengthOf(requests, 6);
    }),
  );

  it.effect("paginates sent metadata, including on a password box", () =>
    Effect.gen(function* () {
      const firstPage = sentRecords(Array.from({ length: 1000 }, (_, index) => sentRecord(index + 1)).join(""));
      const secondPage = sentRecords(sentRecord(1001));
      const { events, requests } = yield* setup(
        (_config, request) => Effect.succeed(request.envelope.includes("<dmOffset>1</dmOffset>") ? firstPage : secondPage),
        "password",
      );
      const opened = yield* events.open(`${uri}?direction=sent&recipient=xyz9876&subject=p%C5%98%C3%8Dloha`, principal);
      const rows = yield* opened.read;
      assert.lengthOf(rows, 1001);
      assert.deepStrictEqual(rows.at(-1), {
        id: "1001",
        direction: "sent",
        sender: recipientId,
        recipient: "xyz9876",
        deliveredAt: notifiedAt,
      });
      assert.deepStrictEqual(requests.map(({ request }) => request.operation), [sentOperation, sentOperation]);
      assert.include(requests[0]?.request.envelope, `<dmToTime>${now}</dmToTime>`);
      assert.include(requests[1]?.request.envelope, "<dmOffset>1001</dmOffset>");
    }),
  );

  it.effect("checks current OAuth permissions, box presence and certificate mode without touching ISDS", () =>
    Effect.gen(function* () {
      const { events, requests, registry } = yield* setup(() => Effect.die("unexpected ISDS call"), "password");
      const sent = `${uri}?direction=sent`;
      assert.match(yield* Effect.flip(events.open(uri, principal)), /system certificate/);
      assert.match(yield* Effect.flip(events.open(sent, { ...principal, clientId: undefined })), /OAuth grant/);
      assert.match(yield* Effect.flip(events.open(sent, { ...principal, scopes: [] })), /isds:read/);
      assert.match(yield* Effect.flip(events.open(sent, { ...principal, resources: [] })), /grant does not cover/);
      assert.match(yield* Effect.flip(events.open(sent, { ...principal, settings: {} })), /accepted Datovka read consent/);
      for (const query of ["direction=any", "sender=short", "subject=%Z1", "sender=xyz9876&sender=abc2def", "unknown=1"]) {
        assert.match(yield* Effect.flip(events.open(`${uri}?${query}`, principal)), /valid direction/);
      }
      const opened = yield* events.open(sent, principal);
      yield* registry.remove(boxId);
      assert.match(yield* Effect.flip(events.open(sent, principal)), /grant does not cover/);
      assert.match(yield* Effect.flip(opened.read), /grant does not cover/);
      assert.isEmpty(requests);
    }),
  );

  it.effect("retries a failed poll and explains missing registration without returning the ISDS status text", () =>
    Effect.gen(function* () {
      const unregistered = ok(soapResponse(notificationOperation, dmStatus("1273", "Untrusted detail")));
      let next = unregistered;
      const { events, requests } = yield* setup(() => Effect.succeed(next));
      const opened = yield* events.open(uri, principal);
      assert.strictEqual(
        yield* Effect.flip(opened.read),
        "This data box must be registered in ISDS for received-message notifications.",
      );
      next = notifications(notification("101"));
      assert.deepStrictEqual((yield* opened.read).map(({ id }) => id), ["101"]);
      assert.deepStrictEqual(requests.map(({ request }) => request.operation), [notificationOperation, notificationOperation]);
    }),
  );

  it.effect("fails an incomplete notification response whose cursor cannot advance", () =>
    Effect.gen(function* () {
      const { events, requests } = yield* setup(() => Effect.succeed(notifications("", true)));
      const opened = yield* events.open(uri, principal);
      assert.strictEqual(yield* Effect.flip(opened.read), "Could not read Datovka message events from ISDS.");
      assert.lengthOf(requests, 1);
    }),
  );
});
