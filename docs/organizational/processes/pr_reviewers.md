# PR Reviewers

Every PR gets **2 reviewers** assigned automatically by the [`assign_reviewers.yml`](../../../.github/workflows/assign_reviewers.yml) action.

## How Reviewers Are Assigned

1. A PR is opened or marked ready for review. The action matches the changed files against [`CODEOWNERS`](../../../.github/CODEOWNERS). Every matching rule is an **area**.
2. The initial 2 reviewers are the owners of these areas who are in a sub-team of `Maintainer`. Members of `Maintainer` without a sub-team are never picked. Excluded are the PR author, commit authors, people with GitHub status **Busy** and people without write access.
3. The action picks the pair that covers the most areas. Ties are broken by **load**: review requests on PRs created in the last 30 days plus pending requests.
4. The pair is requested and the `CODEOWNERS` team requests are removed.

```
# Example: PR touches nes-nautilus/, nes-memory/, nes-runtime/
@frodo     Execution, Memory    load 3   ✅
@sam       Runtime              load 1   ✅
@smeagol   Execution, Runtime   load 6   ❌ higher load
```

If two people cannot cover all areas, the uncovered areas are listed in the job summary.

## Single Owner

If all changed files belong to one owner, or it has only one eligible member, the action requests:

1. The owner's member with the lowest load.
2. The member with the lowest load of a random sub-team of `Maintainer`, who is not an owner of the changed files. `Maintainer` itself is never picked. If the owner is `Maintainer`, which includes all sub-teams, any sub-team member other than the first reviewer can be picked.

```
# Example: PR touches only nes-memory/
@sam       Memory                       load 1   ✅
@merry     Plugins (random team)        load 2   ✅
```

The random team is seeded with the PR number, so reruns pick the same team.

## Named Code Owners

Users named directly in `CODEOWNERS` are always requested when their files change, in addition to the 2 reviewers. This also applies if they are Busy or cannot approve, since their feedback is still expected. This is the only case where a PR gets more than 2 reviewers.

```
# Example: PR touches nes-memory/ and nes-output-formatters/
@sam       Memory                       load 1   ✅
@frodo     InputOutput                  load 2   ✅
@pippin    named in CODEOWNERS                   ✅ added
```

## Manual Requests

Wait for the automatic assignment before requesting anyone manually so that it does not interfere with the assignment algorithm. After that, reviewers can be added or swapped freely, including more than 2.

The action only does the initial assignment. If someone other than a named code owner is already requested or has reviewed, it does nothing.

## Setup

The default workflow token cannot read team members, so the action authenticates as the `NES-Reviewer-Assignment` GitHub App. The app is owned by the organization, installed on this repository only and has these permissions:

- Organization: **Members** read
- Repository: **Contents** read, **Pull requests** read and write

Its credentials are stored in the repository settings:

- Variable `REVIEWER_ASSIGNMENT_APP_ID`: the App ID
- Secret `REVIEWER_ASSIGNMENT_APP_PRIVATE_KEY`: the private key

**Team auto-assignment must be disabled** for all teams in `CODEOWNERS`, otherwise GitHub assigns its own reviewers and the action skips the PR.
The setting is found at Org → Teams → each CODEOWNERS team → Settings → Code review. 
Otherwise GitHub picks its own reviewers alongside the workflow. 

## Notes

- Draft PRs are skipped until they are marked ready for review.
- Every team in `CODEOWNERS` must be `Maintainer` or one of its sub-teams, otherwise the action fails.
