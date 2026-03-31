## Git Workflow for This Context Repo

NEVER push directly to master/main. Always use pull requests:

1. Create a feature branch: `git checkout -b <descriptive-branch-name>`
2. Commit changes to the branch
3. Push the branch: `git push -u origin <branch-name>`
4. Create a PR: `gh pr create --title "..." --body "..."`
5. Share the PR URL with the user

This applies to ALL changes including documentation, CLAUDE.md updates, infrastructure skills, specs, and plans.
