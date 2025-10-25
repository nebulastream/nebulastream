# What is the purpose of this file?
The purpose of this file is to provide a technical and a non-technical checklist for the PR author(s) and reviewers.
The technical checklist includes items related to the code changes, such as the purpose of the change, the change log, and the impact of the change on different parts of the system. 
The non-technical checklist includes items related to the PR itself, such as issue numbers, commit organization, and documentation quality.

# Non-Technical Checklist
- The PR title is descriptive, concise and follows our naming scheme below:
  - `Fix(IssueNumber) QueryCompiler: Nullptr when doing the thing`
  - `Critical Fix(IssueNumber) QueryCompiler: 2 Nullptr when doing the thing`
  - `Chore(IssueNumber) Global: Clang-Formatting changes`
  - `Feature(IssueNumber): Adds destroy function to feature`
  - `Documentation(IssueNumber): Adds documentation for feature`
  - `Design Document(IssueNumber): Adds design document for upcoming feature`

- All related issue numbers are linked and the PR is not added to any project or milestone.
- The commits are organized logically, squashed if necessary, and are properly named. Meaning that it is clear what each commit does and removing certain commits does not break the build.
- All methods are easy to understand, either by their name or documentation.
- The documentation is up-to-date and the documentation has been proofread.

# Technical Checklist
- The changes in this PR are related to the linked issue(s).
- The purpose of the change is clearly described in the PR description, such that it is clear what the change does and why it is necessary.
- All affected components have been added to the PR text, e.g., `QueryEngine: Added a new function x` or `Network Stack: Replaced XYZ`. 
- The changes are covered by tests, either Unittests, Integrationtests, End-to-endtest or via a script that is part of this PR.
- The code aims for high-quality C++ code, e.g., RAII, operator overloading, the STL, and appropriate use of smart-pointers, as described in our coding guidelines.
