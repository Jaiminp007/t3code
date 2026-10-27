import {
  defineSourceControlDriver,
  withOwnServices,
} from "@t3tools/source-control-core/server/driver";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { definition } from "../shared/definition.ts";

import * as GitHubHttpSocket from "./gitHubHttpSocket.ts";
import * as GitHubMediaFetch from "./gitHubMediaFetch.ts";
import * as GitHubPullRequestApi from "./GitHubPullRequestApi.ts";
import * as GitHubPullRequestProvider from "./GitHubPullRequestProvider.ts";
import * as GitHubRest from "./GitHubRest.ts";
import * as GitHubSourceControlProvider from "./GitHubSourceControlProvider.ts";

// Built once so every GitHub reader shares the quota reserve and gh's `http_unix_socket`.
const services = GitHubPullRequestApi.layer.pipe(
  Layer.provideMerge(GitHubRest.layerWithDependencies),
  Layer.provideMerge(GitHubHttpSocket.layer),
);

const makeInstance = Effect.all({
  sourceControl: GitHubSourceControlProvider.make,
  discovery: GitHubSourceControlProvider.makeDiscovery,
  pullRequests: GitHubPullRequestProvider.make,
  fetchMedia: GitHubMediaFetch.make,
});

export const driver = defineSourceControlDriver({
  kind: definition.kind,
  definition,
  make: withOwnServices(makeInstance, services),
});
