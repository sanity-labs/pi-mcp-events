# Pi MCP Events

Use `mise exec --` for Node/npm commands. Run `npm run validate` before PR handoff. Default checks are self-contained and must not require Mellon, cloud credentials, or a model API. Keep `.local/`, credentials, generated certificates, archives, and build output out of commits.

Keep this package limited to the pinned MCP Events contract and Pi delivery. Application tools and server-side History polling belong elsewhere. Test protocol/client, session storage, and Pi integration independently before adding abstractions between them.

Use feature branches and draft pull requests; never push directly to `main`. Preserve intentional, testable component commits. Fold draft review fixes into the appropriate component rather than accumulating fixups. Do not rewrite bot-authored commits. Link each PR to a relevant Linear issue assigned to the author and link the issue back to the PR.

The repository and package start private. Keep `private: true` and do not add publishing credentials or change visibility without explicit approval. Before public release, verify scope ownership, compatibility, dependencies, and licensing.
