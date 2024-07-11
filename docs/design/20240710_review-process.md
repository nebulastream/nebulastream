# The Problem

PR Review process can be labor intensive, and thus it is not uncommon that the PR process is either postponed, or performed lackluster only on the surface level. PR Reviews focus on easily digestible components, like `formatting`, using `auto` or preferring to `pass by reference`, and rarely focus on the bigger picture of the overall change to the system.

In addition, PRs are rarely tested and verified manually by the reviewer, and test coverage is not strictly enforced. Thus, seemingly important PRs can pass the PR review process without necessary test coverage.

Overall, PR quality is enforced very differently from reviewer to reviewer due to missing strict guidelines for documentation and git hygiene. Commit/PR body is rarely verified, especially after multiple rounds of reviews. Actual changes to the system are not reflected in the commit message, which makes it impossible to reason about historic decisions. Currently, we only support `Squash & Merge,` which at least guarantees a single coherent commit, but it also prevents/hinders smaller self-contained commits.

# Goals and Non-Goals

(**G1**): Lift the burden of the reviewer by eliminating minor or trivially detectable problems before the review even starts.

(**G2**): Remove the need for the reviewer to call out *bad* code or missing test coverage: good Cop, bad Bot.

(**G3**): Enforce equal requirements for all PR: `Definition of Done`, if you pass all these checks, the PR will be merged, no more, no less.

(**G4**): The CI Checks should be reproducible locally to reduce the usage of CI Resources and tighten the feedback loop for debugging CI-related issues.

(**NG1**): Completely automate PR Process: *Current* (we will see about AI) tooling is not able to understand potentially complex changes. Tooling can be applied for local reasoning and detecting *bad* code patterns.

(**NG2**): Fully formalize the PR Process: This will not provide a Step-By-Step on how to do a Pull Request, as changes to the code base are usually very different in shape and form. Requirements towards code quality and enforcement are subject to change, but not on a case-by-case basis, rather for all future PRs

# Proposed Solution

## Automation

The PR-Process will upon creating the PR check basic requirements and *warn* (maybe prevent) about potential issues in the changed code. Automatic tooling includes:

- Check formatting using `Clang-Format`, with a TBD format
- Check rudimentary C++-specific checks on changed code using `Clang-Tidy` the Set if rules need to be decided. Initial tuning of rules may allow overruling of the outcome of `Clang-Tidy`
- Warns about changes to `important files`: BuildSystem, Docker, CI
- Check individual commit messages for formatting: e.g., Commit Title

## Building

The process attempts to build the branch and all tests. With changes to our dependencies, it is now possible to build NebulaStream with sanitizers and the libc++ hardening mode. Depending on the overall CI Resources, we may consider building them all. Building has frequently caused issues regarding the support of C++20 on older Ubuntu Distributions, not having a recent compiler and standard library version. This is especially annoying if multiple round-trips are required to fix all build errors, which only occur after fixing the initial issues.

To reduce the number of round trips required, we build the system with the `keep going` flag, which will not stop the build if errors occur. This will potentially blow up the overall log output, which can be reduced by providing filtered views with all build errors.

## Testing and Test-Coverage

Depending on the resources available, we may decide to run multiple test suites with different build configurations. The testing Design Document should provide further insights. The CI is only concerned with Pass/Fail and provides enough details for the developer involved to investigate the cause of failure.

Test coverage is evaluated after successful test execution; its output is correlated with the PR changes. The threshold for the test coverage requirement is TBD. The CI will provide an opt-out to skip the code coverage requirement, but this requires reasoning and the approval of all reviewers.

The overall test runtime is tracked. Large increases in test runtime are flagged as warnings by the CI.

## Code Ownership

We plan to document and exploit Code Ownership to assign PRs to the developer responsible for the changed component. Code Ownership is documented via a `CODEOWNERS` file in the `.github` folder in the repository, which assigns GitHub usernames to paths in the repository. Currently, every PR can be merged into the main with two approvals from any reviewer. Using Code Ownership guarantees that at least one reviewer is knowledgeable about the component. The second reviewer will be a randomly assigned maintainer to transfer knowledge.

## Commit History

Currently, we don't employ any restrictions on `Commit Messages` as we rely on the `Squash&Merge` action to squash all commits into a single commit before merging into the master; this will usually result in large non-atomic commits.

Formal commit messages are required to quickly determine if a commit on the `main` branch should be merged into the next release or postponed. We Propose following the [Conventional Commit](https://www.conventionalcommits.org/en/v1.0.0/#specification):
```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

An `IDEA` plugin exists to verify commit messages locally: https://github.com/lppedd/idea-conventional-commit. The PR-Process can verify the commit messages using: https://github.com/siderolabs/conform

## Code Quality

We propose using `Clang-Tidy` to assist the code review process. However, this will require fine-tuning the `Clang-Tidy` configuration. We investigated more advanced static analysis tooling like `Sonar` or `CodeQL`, but those are either not free or require NebulaStream to be open-sourced. We keep the list of possible static analysis tools open for future extensions. 

We propose proper SpellChecking for all future changes, requiring us to maintain a list of NES specific terms.

The bot will provide an overview of detected issues. For some issues, `Clang-Tidy` can provide quick fixes that the bot will provide. (clang-tidy-pr-comments)[https://github.com/platisd/clang-tidy-pr-comments]
Not all issues flagged by `Clang-Tidy` will be valuable. Some of them might even be incorrect. At least for the initial tuning of rules, it may be possible to overrule the bot.
More advanced static analysis tools can better categorize these issues, providing a notion of severity that could, in the future, dictate if the suggestion is irrelevant or mandatory.

Basic naming conventions are enforced using `Clang-Tidy`, e.g., class names use CamelCase. 

# Alternatives

**Open for Alternatives!**

