> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Automation Bugs

## Triage Comment Extraction Bug (2026-04-16)

**Issue**: Auto-triage script posts incomplete comments (e.g., just `"and"` instead of full triage analysis).

**Root cause**: Extraction logic in `/server/tools/jira_tool.py` (line ~309) fails to correctly parse comment text between delimiters:
```
===TRIAGE_COMMENT_START===
<comment text>
===TRIAGE_COMMENT_END===
```

**Impact**: APA-144575 received only `"and"` comment on 2026-04-16 instead of full root-cause analysis. Leaves tickets partially triaged, requiring manual follow-up.

**Files involved**:
- `server/tools/jira_prompt.py` (lines 143-145: delimiter format definition)
- `server/tools/jira_tool.py` (line ~309: extraction logic)

**Example ticket**: APA-144575 (Premium inference flow — externally set to success mid-run)
