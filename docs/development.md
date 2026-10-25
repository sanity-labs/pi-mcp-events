# Development and review

Use the Node version in `mise.toml` and the committed npm lockfile. `npm ci` also compiles the package. Pi is a host-provided peer dependency; its exact tested version is a development dependency. The library dependencies are installed with the package.

```sh
mise exec -- npm run check
mise exec -- npm test
mise exec -- npm run test:package
```

The first command checks TypeScript. Unit tests cover protocol, transport, webhook, storage, configuration, and Pi lifecycle behavior. The package test builds an npm archive, checks its contents, installs it in an independent temporary consumer without Pi, imports its client API, and registers the installed directory through Pi’s CLI in an isolated agent directory. It then loads the compiled extension in stock Pi with a deterministic model and a synthetic HTTP MCP server.

The synthetic fixture binds only to loopback and uses a temporary certificate trusted explicitly by its webhook sender. It verifies a signed challenge, sends signed events through an HTTPS proxy, and checks saved-session recovery and `--no-session` behavior. It needs no external tunnel or model calls. Temporary files, certificates, listeners, and subscriptions are removed when the test ends. This proves client interoperability with a controlled fixture, not exhaustive server conformance.

For an individual component, pass its test file to `npm test -- test/wire.test.ts`. `npm run validate` is the same complete check used by CI. OpenSSL must be on PATH for the package test.

## Optional Mellon integration

A separate Mellon development fixture can produce a private JSON connection file containing `url`, `baseUrl`, `token`, and `callbackOrigin`. For a proxy receiver, also provide `callbackPort` and `callbackTls: false`. `webhookEventName` selects a separate webhook event, and `webhookReplay: false` marks a best-effort source. The connection file is a credential: keep it under ignored `.local/` and never commit it.

```sh
LOCAL_EVENTS_CONNECTION_FILE="$PWD/.local/mellon-connection.json" npm run test:mellon
```

Direct local TLS additionally uses `LOCAL_EVENTS_TLS_DIR` (default `.local/tls`) containing `localhost.key` and `localhost.crt`. `LOCAL_EVENTS_EXTENSION_PATH` can select another installed package. Tests use the same callback port sequentially; do not run them beside an interactive watcher using that port. `test/local.mjs` is an optional manual helper for an already-running Mellon fixture.

The earlier prototype was exercised against Mellon's ephemeral staging fixture and real staging Redis, including recovery, and with native ChatGPT webhook receivers. Those server results are separate evidence. The self-contained tests in this repository do not require or claim a production Mellon rollout.

## Pull requests

Create a feature branch from current `main`. Open a draft PR with a linked Linear issue assigned to the author, explain the component boundaries, and include validation results. Keep commits independently testable. Fold review fixes into the component they belong to while the PR remains a draft. Do not squash intentional component boundaries or rewrite bot-authored commits.

`main` requires a PR, the `validate` check, an up-to-date branch, and resolved conversations, including for administrators. Force pushes and branch deletion are disabled for `main`. Merge commits preserve the component history. No automated merge or npm publishing workflow is configured. Review approval and merging remain human decisions.

## Private now, public later

During iteration, use checkout installs (clone, run `npm ci`, then `pi install /path/to/checkout`) or tarballs. Direct `pi install git:...` is not supported yet: Pi omits development dependencies when installing a git source, so it cannot compile this checkout. `private: true` blocks npm publication; the private GitHub repository controls source access. The package name does not establish that the matching npm scope is available or configured.

Before public distribution, review the API and pinned-draft compatibility, confirm npm scope ownership, choose the initial prerelease version, review dependency advisories and licensing, and explicitly approve public repository visibility and publishing. Then remove the publication block and add a separately reviewed release workflow. The private CI workflow has no publishing credentials.

Cursor keys from the earlier uncommitted prototype are not migrated. The first resume with this package starts those watches from now; accepted inbox observations remain available. Subsequent resumes use the per-watch checkpoints.

The pinned stock-Pi development dependency currently brings npm advisories for Undici. It is not bundled with this package; compatibility tests use stock Pi without dependency overrides. Reassess the supported host version before public release.
