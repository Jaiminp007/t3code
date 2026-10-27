// @effect-diagnostics nodeBuiltinImport:off - fetch cannot dial a unix socket; gh's http_unix_socket needs node:http.

import * as NodeHttp from "node:http";
import * as NodeStream from "node:stream";

import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HttpClient, HttpClientError, HttpClientRequest, HttpClientResponse } from "effect/http";

import * as HostProcess from "@t3tools/shared/HostProcess";
import * as SourceControlHost from "@t3tools/source-control-core/server/SourceControlHost";

import { GH_PROCESS_CONCURRENCY } from "./GitHubCredentials.ts";

const SKIP_HEADERS = new Set(["connection", "content-length", "host", "transfer-encoding"]);

/** gh host for a GitHub API URL. Other URLs stay on the normal client. */
function gitHubHost(url: URL): string | null {
  const host = url.hostname.toLowerCase();
  if (host === "api.github.com") return "github.com";
  if (host.startsWith("api.") && host.endsWith(".ghe.com")) return host.slice(4);
  if (url.pathname === "/api/graphql" || url.pathname.startsWith("/api/v3")) return host;
  return null;
}

function transportError(request: HttpClientRequest.HttpClientRequest, cause: unknown) {
  return new HttpClientError.HttpClientError({
    reason: new HttpClientError.TransportError({
      cause,
      description: "Could not reach the GitHub HTTP unix socket.",
      request,
    }),
  });
}

// fetch cannot take a socket path. One plain-HTTP hop; the caller's client still follows redirects
// when no socket is set. Runs under the caller's span.
const sendOverSocket = Effect.fnUntraced(function* (
  request: HttpClientRequest.HttpClientRequest,
  url: URL,
  socketPath: string,
  signal: AbortSignal,
) {
  if (request.body._tag !== "Empty" && request.body._tag !== "Uint8Array") {
    return yield* transportError(request, request.body._tag);
  }
  const body = request.body._tag === "Uint8Array" ? request.body : null;
  const headers: NodeHttp.OutgoingHttpHeaders = { connection: "close", host: url.host };
  for (const [name, value] of Object.entries(request.headers)) {
    if (typeof value === "string" && !SKIP_HEADERS.has(name)) headers[name] = value;
  }
  if (body !== null) {
    headers["content-length"] = body.contentLength;
    headers["content-type"] = body.contentType;
  }
  const incoming = yield* Effect.tryPromise({
    try: (abort) =>
      new Promise<NodeHttp.IncomingMessage>((resolve, reject) => {
        const outgoing = NodeHttp.request(
          {
            agent: false,
            headers,
            method: request.method,
            path: `${url.pathname}${url.search}`,
            signal: AbortSignal.any([signal, abort]),
            socketPath,
          },
          resolve,
        );
        outgoing.once("error", reject);
        if (body === null) outgoing.end();
        else outgoing.end(body.body);
      }),
    catch: (cause) => transportError(request, cause),
  });
  const responseHeaders = new Headers();
  for (const [name, value] of Object.entries(incoming.headers)) {
    if (typeof value === "string") responseHeaders.set(name, value);
    else if (Array.isArray(value)) responseHeaders.set(name, value.join(", "));
  }
  return HttpClientResponse.fromWeb(
    request,
    new Response(NodeStream.Readable.toWeb(incoming) as ReadableStream<Uint8Array>, {
      headers: responseHeaders,
      status: incoming.statusCode ?? 0,
    }),
  );
});

export const layer = Layer.effect(
  HttpClient.HttpClient,
  Effect.gen(function* () {
    const fallback = yield* HttpClient.HttpClient;
    const runner = (yield* SourceControlHost.SourceControlHost).process;
    const cwd = yield* HostProcess.WorkingDirectory;
    // Ask gh once per host. A read on every API call would queue on the shared gh limit.
    // `gh config get` applies host-then-global and GH_CONFIG_DIR.
    const sockets = new Map<string, string | null>();
    const socketFor = Effect.fnUntraced(function* (host: string) {
      if (sockets.has(host)) return sockets.get(host) ?? null;
      const socket = yield* runner
        .run({
          args: ["config", "get", "http_unix_socket", "--host", host],
          command: "gh",
          concurrency: GH_PROCESS_CONCURRENCY,
          cwd,
          env: { GH_DEBUG: "", GH_PROMPT_DISABLED: "1" },
          operation: "GitHubHttpSocket.read",
          timeoutMs: 10_000,
        })
        .pipe(
          Effect.map((output) => output.stdout.trim() || null),
          Effect.orElseSucceed(() => null),
        );
      sockets.set(host, socket);
      return socket;
    });
    const execute = Effect.fnUntraced(function* (
      request: HttpClientRequest.HttpClientRequest,
      url: URL,
      signal: AbortSignal,
    ) {
      const host = gitHubHost(url);
      const socketPath = host === null ? null : yield* socketFor(host);
      if (socketPath === null) return yield* fallback.execute(request);
      return yield* sendOverSocket(request, url, socketPath, signal);
    });
    return HttpClient.make((request, url, signal) => execute(request, url, signal));
  }),
);
