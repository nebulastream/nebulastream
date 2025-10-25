# Description
This document contains useful git commands that we use regularly.
The sections below describe different scenarios. For each scenario, we provide at least one git/shell command to handle it.

## Updating outdated feature branch with main
### Scenario:
- you work on a feature branch
- you have several commits that exist only on your feature branch
- your feature branch is not up-to-date with main, i.e., main contains potentially many commits that are missing from your feature branch
### Goal (what you (might) want):
- your branch reflects the current main branch, with all or a selection of your commits on top of the latest commit from main (HEAD)
### Requirements
- you know the revision/hash number of the first commit that you want to apply from your feature branch
- ways to determine the revision number:
  1. in CLion, go to the git tabs (branch symbol on the very bottom left), double-click your local branch, right-click the first commit to apply, choose `Copy Revision number`
  2. go to the main page of the repo, choose your branch, click on `Commits`, find the first commit you want to apply, copy the full SHA
  3. navigate to the git repo in a terminal, run `git reflog` and find the first occurrence of commit you want to apply, copy the revision number on the left 
### Command
```shell
git fetch origin #make sure your branches are up-to-date
git rebase --onto main REVISION_NUMBER_OF_FIRST_COMMIT_THAT_YOU_WANT_TO_APPLY^ YOUR_BRANCH #apply your commits
```
- takes main, applies all commits, starting with `REVISION_NUMBER_OF_FIRST_COMMIT_THAT_YOU_WANT_TO_APPLY` from `YOUR_BRANCH` reflects rebase result in `YOUR_BRANCH`
- leaves the `main` branch untouched
- Note: you need to force push afterwards `git push -f`
