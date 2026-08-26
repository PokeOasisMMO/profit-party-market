import {
  HubConnectionBuilder,
  HttpTransportType,
  LogLevel,
} from "@microsoft/signalr";

const token = process.env.PROJECTX_SESSION_TOKEN;
const contractId = process.env.PROJECTX_CONTRACT_ID;

function emit(payload) {
  process.stdout.write(`${JSON.stringify(payload)}\n`);
}

if (!token || !contractId) {
  emit({
    kind: "fatal",
    error: "The ProjectX bridge is missing its session token or contract ID.",
  });
  process.exit(2);
}

const connection = new HubConnectionBuilder()
  .withUrl("https://rtc.topstepx.com/hubs/market", {
    skipNegotiation: true,
    transport: HttpTransportType.WebSockets,
    accessTokenFactory: () => token,
    timeout: 10_000,
  })
  .configureLogging(LogLevel.Error)
  .withAutomaticReconnect([0, 1_000, 3_000, 5_000, 10_000])
  .build();

const subscriptions = [
  ["quotes", "SubscribeContractQuotes", "UnsubscribeContractQuotes"],
  ["trades", "SubscribeContractTrades", "UnsubscribeContractTrades"],
  ["depth", "SubscribeContractMarketDepth", "UnsubscribeContractMarketDepth"],
];

let stopping = false;
let subscriptionGeneration = 0;
const received = { quotes: false, trades: false, depth: false };

// ProjectX can send the initial DOM snapshot as one very large array. Emit
// every record separately so Python's line reader never exceeds its maximum
// line length and the gateway can apply the Reset, asks, and bids in order.
function emitMarketEvents(target, eventContractId, data) {
  const records = Array.isArray(data) ? data : [data];
  for (const record of records) {
    if (record && typeof record === "object") {
      emit({ kind: "event", target, contractId: eventContractId, data: record });
    }
  }
}

connection.on("GatewayQuote", (eventContractId, data) => {
  received.quotes = true;
  emitMarketEvents("GatewayQuote", eventContractId, data);
});

connection.on("GatewayTrade", (eventContractId, data) => {
  received.trades = true;
  emitMarketEvents("GatewayTrade", eventContractId, data);
});

connection.on("GatewayDepth", (eventContractId, data) => {
  received.depth = true;
  emitMarketEvents("GatewayDepth", eventContractId, data);
});

async function subscribe({ missingOnly = false, attempt = 0 } = {}) {
  for (const [stream, method, unsubscribeMethod] of subscriptions) {
    if (missingOnly && received[stream]) continue;
    try {
      if (missingOnly) {
        await connection.invoke(unsubscribeMethod, contractId);
      }
      await connection.invoke(method, contractId);
      emit({ kind: "subscription", stream, state: "subscribed", attempt });
    } catch (error) {
      emit({
        kind: "subscription",
        stream,
        state: "error",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
}

function scheduleMissingEventRetries() {
  const generation = ++subscriptionGeneration;
  for (const [delay, attempt] of [[4_000, 1], [12_000, 2]]) {
    setTimeout(async () => {
      if (stopping || generation !== subscriptionGeneration) return;
      await subscribe({ missingOnly: true, attempt });
    }, delay);
  }
}

connection.onreconnecting((error) => {
  emit({
    kind: "connection",
    state: "reconnecting",
    error: error instanceof Error ? error.message : error ? String(error) : null,
  });
});

connection.onreconnected(async () => {
  received.quotes = false;
  received.trades = false;
  received.depth = false;
  emit({ kind: "connection", state: "connected" });
  await subscribe();
  scheduleMissingEventRetries();
});

connection.onclose((error) => {
  emit({
    kind: stopping ? "connection" : "fatal",
    state: "closed",
    error: error instanceof Error ? error.message : error ? String(error) : null,
  });
});

async function shutdown() {
  if (stopping) return;
  stopping = true;
  try {
    await connection.stop();
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

try {
  await connection.start();
  emit({ kind: "connection", state: "connected" });
  await subscribe();
  scheduleMissingEventRetries();
} catch (error) {
  emit({
    kind: "fatal",
    error: error instanceof Error ? error.message : String(error),
  });
  process.exit(1);
}
