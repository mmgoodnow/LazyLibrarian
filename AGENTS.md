# Git workflow notes

Remotes:
- origin: upstream GitLab (LazyLibrarian/LazyLibrarian)
- gh: GitHub fork (mmgoodnow/LazyLibrarian)
- fork: GitLab fork (mmgoodnow/LazyLibrarian)

Preferred workflow:
- Start from `origin/master` locally.
- Make commits on a local branch.
- Rebase `gh/master` onto `origin/master` (history rewrite is OK on the fork).
- Cherry-pick the new commits onto `gh/master`, then force-push once.
- Cherry-pick the same commits onto a `fork/<branch>` and optionally open a draft MR with `glab`.

To avoid pushing twice to GitHub, do the rebase/cherry-picks locally first, then a single
`git push --force-with-lease gh master`.
