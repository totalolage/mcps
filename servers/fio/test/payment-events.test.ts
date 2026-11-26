import { assert, describe, it } from "@effect/vitest";
import { LOCAL_PRINCIPAL, type Principal } from "@mcps/core/transport/principal.js";
import { Deferred, Effect, Fiber, Redacted, Ref } from "effect";
import { TestClock } from "effect/testing";
import { Accounts, makeAccounts, toAccount } from "../src/accounts.js";
import { FioServerError } from "../src/errors.js";
import { makePaymentEvents } from "../src/mcp/payment-events.js";
import { SCOPE_READ } from "../src/mcp/permissions.js";
import { normalizeTransaction, type Transaction } from "../src/statement.js";
import { fakeFio, type PeriodsScript, STATEMENT } from "./support/fio-api.js";

const account = (name: string, token: string, id = name) =>
  toAccount({ name, token: Redacted.make(token), accountNumber: undefined, currency: undefined }, id);

const primary = account("Operating + CZK", "fake-payments-a");
const secondary = account("Savings", "fake-payments-b");
const uri = "fio://payments/Operating%20%2B%20CZK";
const granted: Principal = { subject: "grant-a", scopes: [SCOPE_READ], resources: [primary.tokenFingerprint] };

const movement = (id: number, changes: Partial<Transaction> = {}): Transaction => ({
  ...normalizeTransaction({}),
  id,
  date: "2026-07-01",
  amount: -12.5,
  currency: "CZK",
  variableSymbol: "0012",
  counterpartyAccount: "123-456",
  counterpartyBankCode: "2010",
  ...changes,
});

const statement = (...transactions: Transaction[]) => ({ ...STATEMENT, transactionCount: transactions.length, transactions });

const setup = Effect.fnUntraced(function* (script: PeriodsScript) {
  const accounts = yield* makeAccounts([primary, secondary]);
  const fio = yield* fakeFio(script);
  const events = yield* makePaymentEvents().pipe(Effect.provideService(Accounts, accounts), Effect.provide(fio.layer));
  return { accounts, events, calls: fio.calls };
});

describe("Fio payment events", () => {
  it.effect("filters booked movements and projects only validated metadata from background /periods", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.UTC(2026, 5, 30, 22, 5)); // July 1 in Prague.
      const harmful = "upstream free text";
      const { events, calls } = yield* setup(() =>
        Effect.succeed(
          statement(
            movement(1, { comment: harmful, messageForRecipient: harmful, counterpartyName: harmful }),
            movement(2, { amount: 12.5 }),
            movement(3, { currency: "EUR" }),
            movement(4, { amount: -12.49 }),
            movement(5, { amount: -12.51 }),
            movement(6, { variableSymbol: "12" }),
            movement(7, { counterpartyBankCode: "0100" }),
            movement(8, { counterpartyAccount: harmful, variableSymbol: harmful, currency: harmful, date: harmful }),
            movement(9, { amount: 0 }),
            movement(10, { id: null }),
          ),
        ),
      );
      const exact = yield* events.open(
        `${uri}?direction=out&currency=CZK&minAmount=12.5&maxAmount=12.5&variableSymbol=0012&counterpartyAccount=123-456%2F2010`,
        granted,
      );
      assert.deepStrictEqual(yield* exact.read, [
        {
          id: "1",
          date: "2026-07-01",
          amount: -12.5,
          direction: "out",
          currency: "CZK",
          variableSymbol: "0012",
          counterpartyAccount: "123-456",
          counterpartyBankCode: "2010",
        },
      ]);
      const inbound = yield* events.open(`${uri}?direction=in`, granted);
      assert.deepStrictEqual((yield* inbound.read).map(({ id }) => id), ["2"]);
      const bankAgnostic = yield* events.open(`${uri}?counterpartyAccount=123-456`, granted);
      assert.deepStrictEqual((yield* bankAgnostic.read).map(({ id }) => id), ["1", "2", "3", "4", "5", "6", "7"]);
      const all = yield* events.open(uri, granted);
      const results = yield* all.read;
      assert.deepStrictEqual(results.map(({ id }) => id), ["1", "2", "3", "4", "5", "6", "7", "8"]);
      assert.isFalse(JSON.stringify(results).includes(harmful));
      assert.strictEqual(all.source, exact.source);
      assert.isFalse(all.source.includes(primary.tokenFingerprint));
      assert.deepStrictEqual(yield* Ref.get(calls), [
        { token: "fake-payments-a", from: "2026-06-25", to: "2026-07-01", priority: "background" },
      ]);
    }),
  );

  it.effect("rejects invalid selectors and current account grants before fetching", () =>
    Effect.gen(function* () {
      const { events, accounts, calls } = yield* setup(() => Effect.succeed(STATEMENT));
      for (const invalid of [
        "fio://payments/",
        "fio://payments/%ZZ",
        `${uri}/extra`,
        `${uri}#fragment`,
        `${uri}?currency=usd`,
        `${uri}?direction=out&direction=out`,
        `${uri}??direction=out`,
        `${uri}?minAmount=10&maxAmount=9`,
        `${uri}?minAmount=-1`,
        `${uri}?maxAmount=0x12`,
        `${uri}?variableSymbol=`,
        `${uri}?counterpartyAccount=not-an-account`,
        `${uri}?unexpected=1`,
      ]) {
        assert.strictEqual(yield* Effect.flip(events.open(invalid, granted)), "Invalid Fio payment event URI or filters.");
      }

      const denied = "The Fio account is unavailable or access was not granted.";
      assert.strictEqual(yield* Effect.flip(events.open(uri, { ...granted, scopes: [] })), denied);
      assert.strictEqual(yield* Effect.flip(events.open("fio://payments/Savings", granted)), denied);
      assert.strictEqual(yield* Effect.flip(events.open("fio://payments/Missing", granted)), denied);

      const previous = yield* events.open(uri, granted);
      const rotated = account(primary.name, "rotated-fake-token", primary.id);
      yield* accounts.replace([rotated]);
      assert.strictEqual(yield* Effect.flip(events.open(uri, granted)), denied);
      assert.strictEqual(yield* Effect.flip(previous.read), denied);
      const authorized = yield* events.open(uri, LOCAL_PRINCIPAL);
      assert.notStrictEqual(authorized.source, previous.source);
      assert.deepStrictEqual(yield* Ref.get(calls), []);
    }),
  );

  it.effect("shares pending reads per token, retries a failed snapshot, and refreshes a recent success", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.UTC(2026, 6, 1));
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const { events, calls } = yield* setup((_call, n) => {
        if (n === 0) {
          return Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(Effect.fail(new FioServerError({ status: 500, excerpt: "private upstream error" }))),
          );
        }
        return Effect.succeed(statement(movement(n)));
      });
      const outgoing = yield* events.open(`${uri}?direction=out`, granted);
      const alias = yield* events.open(uri, LOCAL_PRINCIPAL);
      const first = yield* Effect.forkChild(Effect.flip(outgoing.read));
      yield* Deferred.await(started);
      const second = yield* Effect.forkChild(Effect.flip(alias.read), { startImmediately: true });
      yield* Effect.yieldNow;
      assert.lengthOf(yield* Ref.get(calls), 1);
      yield* Deferred.succeed(release, undefined);
      assert.deepStrictEqual(yield* Effect.all([Fiber.join(first), Fiber.join(second)]), [
        "Could not read new Fio payment movements.",
        "Could not read new Fio payment movements.",
      ]);
      assert.lengthOf(yield* Ref.get(calls), 1);

      assert.deepStrictEqual((yield* outgoing.read).map(({ id }) => id), ["1"]);
      assert.deepStrictEqual((yield* alias.read).map(({ id }) => id), ["1"]);
      assert.lengthOf(yield* Ref.get(calls), 2);

      yield* TestClock.adjust("1 minute");
      assert.deepStrictEqual((yield* alias.read).map(({ id }) => id), ["2"]);
      assert.lengthOf(yield* Ref.get(calls), 3);
    }),
  );

  it.effect("keeps token snapshots separate and rejects a result when the account rotates during a poll", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const { accounts, events, calls } = yield* setup(({ token }) =>
        token === "fake-payments-a"
          ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.as(statement(movement(1))))
          : Effect.succeed(statement(movement(2))),
      );
      const first = yield* events.open(uri, granted);
      const firstPoll = yield* Effect.forkChild(Effect.flip(first.read));
      yield* Deferred.await(started);
      const second = yield* events.open("fio://payments/Savings", LOCAL_PRINCIPAL);
      assert.notStrictEqual(second.source, first.source);
      assert.deepStrictEqual((yield* second.read).map(({ id }) => id), ["2"]);
      yield* accounts.replace([account(primary.name, "rotated-fake-token", primary.id), secondary]);
      yield* Deferred.succeed(release, undefined);
      assert.strictEqual(yield* Fiber.join(firstPoll), "The Fio account is unavailable or access was not granted.");
      assert.deepStrictEqual((yield* second.read).map(({ id }) => id), ["2"]);
      assert.deepStrictEqual((yield* Ref.get(calls)).map(({ token }) => token), ["fake-payments-a", "fake-payments-b"]);
    }),
  );
});
