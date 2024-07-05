# What is the purpose of this file?
The purpose of this file is to provide a checklist for the PR author to ensure that the PR is ready for review. 
This checklist is divided into two parts: a technical checklist and a non-technical checklist. 
The technical checklist includes items related to the code changes, such as the purpose of the change, the change log, and the impact of the change on different parts of the system. 
The non-technical checklist includes items related to the PR itself, such as issue numbers, commit organization, and documentation quality.

# Technical Checklist
- The changes in this PR are related to the linked issue(s).
- The purpose of the change is clearly described in the PR description.
- All affected components have been added to the PR text.
- The changes are covered by tests, either gtests or via a script that is part of this PR.
- The code aims for high-quality C++ code, e.g., RAII, operator overloading, the STL, and appropriate use of smart-pointers. 

# Non-Technical Checklist
- The PR title is descriptive and concise.
- All issue numbers are linked and the PR is not added to any project or milestone.
- The commits are organized logically and are squashed if necessary.
- All necessary methods (no getter, setter, or constructor) have been documented.
- The documentation is up-to-date and the documentation has been proofread.