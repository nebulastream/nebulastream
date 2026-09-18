# How to Contribute
This document collects what is needed to get a code change from your machine into the NebulaStream codebase: what a good PR looks like, and the checklist we use to review it. For everything else you need along the way, see [Build Instructions](development.md), [Testing](testing.md), and the [Coding Guidelines](coding_guidelines.md).

# PR Checklist
The purpose of this checklist is to give PR authors and reviewers a shared technical and non-technical bar. The non-technical checklist covers the PR itself, such as issue numbers, commit organization, and documentation quality. The technical checklist covers the code changes, such as the purpose of the change, its impact on other parts of the system, and test coverage.

## Non-Technical Checklist
- The PR title is descriptive, concise and follows our naming scheme below:
  - `Fix(IssueNumber) QueryCompiler: Nullptr when doing the thing`
  - `Critical Fix(IssueNumber) QueryCompiler: 2 Nullptr when doing the thing`
  - `Chore(IssueNumber) Global: Clang-Formatting changes`
  - `Feature(IssueNumber): Adds destroy function to feature`
  - `Documentation(IssueNumber): Adds documentation for feature`
  - `Design Document(IssueNumber): Adds design document for upcoming feature`
- All related issue numbers are linked and the PR is not added to any project or milestone.
- The commits are organized logically, squashed if necessary, and are properly named. Meaning that it is clear what each commit does and removing certain commits does not break the build.
- Commit messages follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) standard.
- All methods are easy to understand, either by their name or documentation.
- The documentation is up-to-date and the documentation has been proofread.

## Technical Checklist
- The changes in this PR are related to the linked issue(s).
- The purpose of the change is clearly described in the PR description, such that it is clear what the change does and why it is necessary.
- All affected components have been added to the PR text, e.g., `QueryEngine: Added a new function x` or `Network Stack: Replaced XYZ`.
- The changes are covered by tests, either Unittests, Integrationtests, End-to-endtest or via a script that is part of this PR.
- The code aims for high-quality C++ code, e.g., RAII, operator overloading, the STL, and appropriate use of smart-pointers, as described in our [coding guidelines](coding_guidelines.md).
