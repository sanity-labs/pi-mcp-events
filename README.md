# Pi MCP Events

Private iteration of `@sanity-labs/pi-mcp-events`: an Events-only Pi extension targeting MCP revision `2026-07-28` and SEP-3415 at `b0efd116faca84c9caad109ccf0778da4dc49579`. The [pinned draft](docs/events-draft.md) is the contract for this implementation.

This repository is being assembled from the locally validated prototype in independently testable components. The protocol foundation owns schema validation, capability negotiation, HTTP routing metadata, and correlated streams using the official MCP client. It has no Pi runtime dependency. Application-specific tools belong in separate extensions.

```sh
mise install
mise exec -- npm ci
mise exec -- npm run check
mise exec -- npm test
mise exec -- npm run build
```

Publishing is disabled with `private: true`. Public availability and npm distribution are future, explicit release steps. No release workflow is configured.
