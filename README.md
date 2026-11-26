# mcps

A Bun workspace of [MCP](https://modelcontextprotocol.io) servers written with [Effect](https://effect.website), and the core they share.

| Path | Package | What it is |
|---|---|---|
| [`servers/fio`](servers/fio) | `fio-mcp` | MCP server for FIO Bank's REST API. Setup, configuration, deployment and its development notes: [servers/fio/README.md](servers/fio/README.md). |
| [`servers/datova-schranka`](servers/datova-schranka) | `datova-schranka` | MCP server for Czech data boxes (datová schránka, ISDS). Setup, configuration, sending and its development notes: [servers/datova-schranka/README.md](servers/datova-schranka/README.md). |
| [`packages/core`](packages/core) | `@mcps/core` (private) | Server-neutral parts: stdio and HTTP transports, OAuth, the admin shell and grant handling. Servers inject their own resources, scopes and pages. It must not import a server; the `core-import-boundary` lint rule enforces that. |

The root holds what every workspace shares: `package.json` (workspaces, scripts, dev tools), one `bun.lock`, `bunfig.toml` (install policy), `tsconfig.base.json`, `vitest.config.ts`, `biome.json` with the Grit rules in `lint/`, and `scripts/ship-image.sh`.

## MCP event subscriptions

Both servers support `subscriptions/listen` on MCP `2026-07-28`. This revision opens a notification stream on the listen request. Earlier revisions defined `resources/subscribe` and could deliver updates on a separate GET stream over HTTP. Clients can still use the earlier revisions for tools. To receive events from these servers, the client must support the new listen request and its acknowledgment and update notifications. [MCP 2026-07-28 subscriptions](https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/subscriptions) · [MCP 2025-11-25 resource subscriptions](https://modelcontextprotocol.io/specification/2025-11-25/server/resources#subscriptions)

Use `resources/templates/list` to discover the URI template. Expand it with the account or box label and the filters you want. Each URI has its own filter. Use the same URI in `resources/read` after an update. See [Fio payment filters](servers/fio/README.md#payment-subscriptions) and [Datovka message filters](servers/datova-schranka/README.md#message-subscriptions).

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "subscriptions/listen",
  "params": {
    "_meta": {
      "io.modelcontextprotocol/protocolVersion": "2026-07-28",
      "io.modelcontextprotocol/clientCapabilities": {}
    },
    "notifications": {
      "resourceSubscriptions": ["fio://payments/main?direction=in&currency=CZK"]
    }
  }
}
```

Over HTTP, POST the request to `/mcp` with `Content-Type: application/json`, `Accept: application/json, text/event-stream`, `MCP-Protocol-Version: 2026-07-28`, and `Mcp-Method: subscriptions/listen`. Send the bearer token on OAuth deployments. For `resources/read`, send `Mcp-Method: resources/read` and `Mcp-Name` with the resource URI. Fio also supports subscriptions over stdio. Datovka serves them over authenticated HTTP.

The server checks the grant and takes a baseline before it acknowledges the subscription. The baseline is silent. Each stream accepts up to eight URIs. About once a minute, the server polls and sends `notifications/resources/updated` for each URI with new matching IDs. The notification carries the URI and subscription ID. A single update can cover several rows. The server suppresses repeat updates for matching IDs it has already seen.

Read the resource to get a JSON array of matching rows from the provider's recent window. During a provider outage, an active subscription keeps its last successful baseline and retries on the next poll. To unsubscribe, close the HTTP response stream or send `notifications/cancelled` with the listen request ID over stdio. An expired or revoked token, lost access, or a label pointing to a different source ends the subscription.

After a disconnect, listen again and read the resource after acknowledgment to catch up. Keep the last processed IDs on the client if you need to avoid reprocessing rows. The server takes a fresh baseline on each listen and holds no replay log. Recovery from `resources/read` covers the provider's recent window.

## Development

Requires Node.js 22.13 or newer and [Bun](https://bun.com) 1.4.2. Clone the repository and run everything from its root:

```sh
git clone https://github.com/totalolage/mcps.git && cd mcps
bun install              # installs all workspaces, patches TypeScript for Effect diagnostics, builds dist/
bun run build            # tsc -b: packages/core, then servers/fio
bun run typecheck        # tsc with Effect diagnostics
bun run lint             # Biome and the Grit rules in lint/
bun run test             # vitest over every workspace's test/
bun run check            # typecheck, lint and test
bun run generate:check   # the generated FIO client matches openapi/
bun run test:scripts     # tests scripts/ship-image.sh (stand-in docker and ssh) and the core boundary lint rule
```

## Images and CI

Each server has its own Dockerfile and builds from the repository root, e.g. `docker build -f servers/fio/Dockerfile .`. `bun run ship:image` builds the fio-mcp image and streams it to a docker host over ssh; no registry is involved (see [Shipping the image](servers/fio/README.md#shipping-the-image-to-your-server)). `bun run ship:image -- --server datova-schranka` does the same for another server: it builds `servers/<server>/Dockerfile` and tags the image with that workspace's package name and version, e.g. `datova-schranka:<version>-<sha>`.

[CI](.github/workflows/ci.yml) runs the checks on pull requests and pushes to `main`. Each server has a job that runs when its own directory changes, and every job runs when `packages/core` or the shared root tooling changes. CI never builds or ships an image.
