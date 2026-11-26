import { createHmac, randomBytes } from "node:crypto";
import { CurrentPrincipal, type Principal } from "@mcps/core/transport/principal.js";
import { Cache, Effect, Exit } from "effect";
import { Accounts } from "../accounts.js";
import { addDays, pragueToday } from "../cache/dates.js";
import { validAccountName } from "../config.js";
import { FioApi } from "../fio-api.js";
import type { Transaction } from "../statement.js";
import { permittedAccount } from "./permissions.js";
import { BANK_CODE, CURRENCY, CZECH_ACCOUNT, DATE, IBAN, SYMBOL } from "./untrusted-policy.js";

const FILTERS = ["direction", "currency", "minAmount", "maxAmount", "variableSymbol", "counterpartyAccount"];
const URI_TEMPLATE = `fio://payments/{account}{?${FILTERS.join(",")}}`;
const INVALID_URI = "Invalid Fio payment event URI or filters.";
const ACCOUNT_UNAVAILABLE = "The Fio account is unavailable or access was not granted.";
const READ_FAILED = "Could not read new Fio payment movements.";

type Direction = "in" | "out";

interface PaymentSelection {
  readonly account: string;
  readonly direction: Direction | null;
  readonly currency: string | null;
  readonly minAmount: number | undefined;
  readonly maxAmount: number | undefined;
  readonly variableSymbol: string | null;
  readonly counterpartyAccount: string | null;
}

const accountNumber = (value: string): boolean => CZECH_ACCOUNT.test(value) || IBAN.test(value);
const accountWithBank = /^(?:\d{1,6}-)?\d{1,10}\/\d{4}$/;

/** Undefined means omitted. Null means present but invalid. */
function amountBound(value: string | null): number | null | undefined {
  if (value === null) return undefined;
  if (!/^\d+(?:\.\d+)?$/.test(value)) return null;
  const amount = Number(value);
  return Number.isFinite(amount) ? amount : null;
}

function parseSelection(uri: string): PaymentSelection | null {
  const match = /^fio:\/\/payments\/([^/?#]+)(\?[^#]+)?$/.exec(uri);
  if (match?.[1] === undefined) return null;

  let account: string;
  try {
    account = decodeURIComponent(match[1]);
  } catch {
    return null;
  }
  if (!validAccountName(account)) return null;

  const params = new URLSearchParams(match[2]);
  const seen = new Set<string>();
  for (const [name, value] of params) {
    if (!FILTERS.includes(name) || seen.has(name) || value !== value.trim()) return null;
    seen.add(name);
  }

  const direction = params.get("direction");
  const currency = params.get("currency");
  const minAmount = amountBound(params.get("minAmount"));
  const maxAmount = amountBound(params.get("maxAmount"));
  const variableSymbol = params.get("variableSymbol");
  const counterpartyAccount = params.get("counterpartyAccount");
  if (direction !== null && direction !== "in" && direction !== "out") return null;
  if (currency !== null && !CURRENCY.test(currency)) return null;
  if (minAmount === null || maxAmount === null || (minAmount ?? 0) > (maxAmount ?? Infinity)) return null;
  if (variableSymbol !== null && !SYMBOL.test(variableSymbol)) return null;
  if (counterpartyAccount !== null && !accountNumber(counterpartyAccount) && !accountWithBank.test(counterpartyAccount)) {
    return null;
  }
  return { account, direction, currency, minAmount, maxAmount, variableSymbol, counterpartyAccount };
}

interface PaymentEvent {
  readonly id: string;
  readonly date: string | null;
  readonly amount: number;
  readonly direction: Direction;
  readonly currency: string | null;
  readonly variableSymbol: string | null;
  readonly counterpartyAccount: string | null;
  readonly counterpartyBankCode: string | null;
}

function identifier(value: string | null, pattern: RegExp): string | null {
  return value !== null && pattern.test(value) ? value : null;
}

function toPaymentEvent(tx: Transaction): PaymentEvent | null {
  if (tx.id === null || !Number.isSafeInteger(tx.id) || tx.id <= 0) return null;
  if (tx.amount === null || !Number.isFinite(tx.amount) || tx.amount === 0) return null;
  return {
    id: String(tx.id),
    date: identifier(tx.date, DATE),
    amount: tx.amount,
    direction: tx.amount > 0 ? "in" : "out",
    currency: identifier(tx.currency, CURRENCY),
    variableSymbol: identifier(tx.variableSymbol, SYMBOL),
    counterpartyAccount: tx.counterpartyAccount !== null && accountNumber(tx.counterpartyAccount) ? tx.counterpartyAccount : null,
    counterpartyBankCode: identifier(tx.counterpartyBankCode, BANK_CODE),
  };
}

function matches(event: PaymentEvent, selection: PaymentSelection): boolean {
  const amount = Math.abs(event.amount);
  const account = selection.counterpartyAccount;
  return (
    (selection.direction === null || selection.direction === event.direction) &&
    (selection.currency === null || selection.currency === event.currency) &&
    (selection.minAmount === undefined || amount >= selection.minAmount) &&
    (selection.maxAmount === undefined || amount <= selection.maxAmount) &&
    (selection.variableSymbol === null || selection.variableSymbol === event.variableSymbol) &&
    (account === null || account === event.counterpartyAccount || account === `${event.counterpartyAccount}/${event.counterpartyBankCode}`)
  );
}

export const makePaymentEvents = Effect.fnUntraced(function* () {
  const accounts = yield* Accounts;
  const fio = yield* FioApi;
  const sourceKey = randomBytes(32);

  const pending = yield* Cache.makeWith(
    Effect.fnUntraced(function* (fingerprint: string) {
      // Let both caches publish their pending entry before starting the provider.
      yield* Effect.yieldNow;
      const account = (yield* accounts.list).find((candidate) => candidate.tokenFingerprint === fingerprint);
      if (account === undefined) return yield* Effect.fail(ACCOUNT_UNAVAILABLE);
      const today = yield* pragueToday;
      const { transactions } = yield* fio.periods(account, addDays(today, -6), today, "background").pipe(
        Effect.mapError(() => READ_FAILED),
      );
      return transactions.map(toPaymentEvent).filter((event) => event !== null);
    }),
    { capacity: Infinity, timeToLive: () => 0 },
  );
  // Eviction of a retained snapshot must leave any shared request in flight alone.
  const snapshots = yield* Cache.makeWith((fingerprint: string) => Cache.get(pending, fingerprint), {
    capacity: 256,
    timeToLive: (exit) => (Exit.isSuccess(exit) ? "30 seconds" : 0),
  });

  return {
    name: "fio-payments",
    description:
      "Booked Fio account movements from the last seven Prague calendar days. Use the encoded account name and optional combined filters. Direction is in or out, currency uses three uppercase letters, and amount bounds are inclusive magnitudes. Match the variable symbol and counterparty account exactly. Use a Czech account number, optionally with /bank code, or an IBAN.",
    uriTemplate: URI_TEMPLATE,
    open: Effect.fnUntraced(function* (uri: string, principal: Principal) {
      const selection = parseSelection(uri);
      if (selection === null) return yield* Effect.fail(INVALID_URI);
      const resolve = permittedAccount(accounts, selection.account).pipe(
        Effect.provideService(CurrentPrincipal, principal),
        Effect.mapError(() => ACCOUNT_UNAVAILABLE),
      );
      const account = yield* resolve;
      const source = createHmac("sha256", sourceKey).update(JSON.stringify([account.id, account.tokenFingerprint])).digest("hex");
      const sameAccount = Effect.flatMap(resolve, (current) =>
        current.id === account.id && current.tokenFingerprint === account.tokenFingerprint
          ? Effect.void
          : Effect.fail(ACCOUNT_UNAVAILABLE),
      );
      const read = Effect.gen(function* () {
        yield* sameAccount;
        const events = yield* Cache.get(snapshots, account.tokenFingerprint);
        yield* sameAccount;
        return events.filter((event) => matches(event, selection));
      });
      return { source, read };
    }),
  };
});
