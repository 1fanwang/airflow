# PR Standards

> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

## PR Description Template Requirements

LinkedIn's "Pull Request Description" validation check enforces a specific template format. PRs must include these required sections:

- **## Problem & Solution Overview** — Concisely describe the issue/requirement and how your change addresses it
- **## Testing Done** — Description of testing performed

Common failure causes:
- Using alternate section names (e.g., `## Summary` instead of `## Problem & Solution Overview`)
- Omitting required sections
- Sections named with different capitalization or wording

The check is exact: section names must match precisely. If the "Pull Request Description" check fails, verify the PR body uses the exact section headers expected.
