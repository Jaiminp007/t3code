// @effect-diagnostics nodeBuiltinImport:off - The fixture is an HTTP server bound to a unix socket.

import * as NodeHttp from "node:http";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Redacted from "effect/Redacted";
import { GraphQLClient } from "effect/graphql";
import { HttpClient, HttpClientResponse } from "effect/http";

import * as SourceControlRateLimit from "@t3tools/source-control-core/server/SourceControlRateLimit";
import * as TestSourceControlHost from "@t3tools/source-control-testing/TestSourceControlHost";

import * as GitHubCredentials from "./GitHubCredentials.ts";
import * as GitHubGraphQlClient from "./gitHubGraphQlClient.ts";
import * as GitHubHttpSocket from "./gitHubHttpSocket.ts";
import * as GitHubQuota from "./GitHubQuota.ts";
import * as GitHubRest from "./GitHubRest.ts";
import { PullRequestBatchesGraphQLGroup } from "./pullRequestBatches.graphql.ts";

const credentials = Layer.succeed(GitHubCredentials.GitHubCredentials, {
  get: (host) =>
    Effect.succeed({
      fingerprint: `${host}:placeholder`,
      host,
      source: "gh" as const,
      token: Redacted.make("placeholder"),
    }),
  invalidate: () => Effect.void,
});

const clientFor = (
  stdout: string,
  direct: Array<string>,
  calls: Array<ReadonlyArray<string>> = [],
) =>
  GitHubRest.layer.pipe(
    Layer.provideMerge(GitHubHttpSocket.layer),
    Layer.provide(
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make((request) => {
          direct.push(request.url);
          return Effect.succeed(HttpClientResponse.fromWeb(request, Response.json({ ok: true })));
        }),
      ),
    ),
    Layer.provideMerge(credentials),
    Layer.provideMerge(GitHubQuota.layer),
    Layer.provideMerge(SourceControlRateLimit.layer),
    Layer.provide(
      TestSourceControlHost.layer({
        process: {
          run: (input) =>
            Effect.sync(() => {
              calls.push(input.args);
              return TestSourceControlHost.processOutput(stdout);
            }),
        },
      }),
    ),
  );

const serve = (socketPath: string, seen: Array<string>) =>
  Effect.acquireRelease(
    Effect.callback<NodeHttp.Server, globalThis.Error>((resume) => {
      const server = NodeHttp.createServer((request, response) => {
        request.resume();
        seen.push(
          `${request.method} ${request.url} ${request.headers.host} ${request.headers.authorization} ${request.headers["content-type"] ?? "-"}`,
        );
        response.writeHead(200, { "content-type": "application/json" });
        response.end(
          request.url === "/graphql"
            ? '{"data":{"repository":{"pullRequest":{"additions":3,"deletions":1}}}}'
            : '{"login":"octocat"}',
        );
      });
      server.once("error", (cause) => resume(Effect.fail(cause)));
      server.listen(socketPath, () => resume(Effect.succeed(server)));
      return Effect.sync(() => server.close());
    }),
    (server) =>
      Effect.callback<void>((resume) => {
        server.closeAllConnections();
        server.close(() => resume(Effect.void));
      }),
  );

describe("GitHub HTTP unix socket", () => {
  it.live("sends REST and GraphQL through the socket from gh config get", () =>
    Effect.gen(function* () {
      const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped({
        prefix: "t3-gh-socket-",
      });
      const socketPath = (yield* Path.Path).join(directory, "gh.sock");
      const seen: Array<string> = [];
      const direct: Array<string> = [];
      const calls: Array<ReadonlyArray<string>> = [];
      yield* serve(socketPath, seen);
      const stats = yield* Effect.gen(function* () {
        const user = yield* (yield* GitHubRest.GitHubRest).rest({
          host: "github.com",
          operation: "user",
          path: "user",
        });
        expect(user.body).toContain("octocat");
        const client = yield* GraphQLClient.make(
          PullRequestBatchesGraphQLGroup.middleware(GitHubGraphQlClient.GitHubGraphQlAuth),
        ).pipe(Effect.provide(GitHubGraphQlClient.layer));
        return yield* client
          .PullRequestStats({ name: "web", number: 7, owner: "acme" })
          .pipe(GitHubGraphQlClient.call({ host: "github.com", operation: "stats" }));
      }).pipe(Effect.provide(clientFor(`${socketPath}\n`, direct, calls)));
      expect(stats.repository?.pullRequest).toMatchObject({ additions: 3, deletions: 1 });
      expect(seen).toEqual([
        "GET /user api.github.com Bearer placeholder -",
        "POST /graphql api.github.com Bearer placeholder application/json",
      ]);
      expect(direct).toEqual([]);
      expect(calls).toEqual([["config", "get", "http_unix_socket", "--host", "github.com"]]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("keeps direct https when gh reports no socket", () => {
    const direct: Array<string> = [];
    return Effect.gen(function* () {
      const user = yield* (yield* GitHubRest.GitHubRest).rest({
        host: "github.com",
        operation: "user",
        path: "user",
      });
      expect(user.body).toContain("ok");
      expect(direct).toEqual(["https://api.github.com/user"]);
    }).pipe(Effect.provide(clientFor("", direct)));
  });

  it.live("fails when the socket cannot be reached and does not call api.github.com", () => {
    const direct: Array<string> = [];
    return Effect.gen(function* () {
      const failure = yield* (yield* GitHubRest.GitHubRest)
        .rest({ host: "github.com", operation: "user", path: "user" })
        .pipe(Effect.flip);
      expect(failure._tag).toBe("GitHubApiRequestError");
      expect(direct).toEqual([]);
    }).pipe(Effect.provide(clientFor("/tmp/t3-gh-socket-missing.sock", direct)));
  });
});
