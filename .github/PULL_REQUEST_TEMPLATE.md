## Purpose of the Change and Brief Change Log
**(for example:)** 
This pull request adds support for variable sized data to the nested loop join. The change list is as follows:
- Added a `write()` and `read()` method to the `PagedVector` that supports variable sized data.
- Changed the `NLJBuild` to call the `write()` method when adding a tuple to the `PagedVector`.
- Changed the `NLJProbe` to call the `read()` method when reading a tuple from the `PagedVector`.

## Verifying this change
This change is tested by
*(for example:)*
- *Added integration tests for end-to-end deployment with large payloads (100MB)*
- *Extended integration test for recovery after main failure*
- *Added test that validates that TaskInfo is transferred only once across recoveries*

## What components does this pull request potentially affect?
*(for example:)*
- Dependencies (does it add or upgrade a dependency)
- ExecutionEngine
- QueryCompiler
- QueryEngine
- RestAPI

## Documentation
- The change is reflected in the necessary documentation, e.g., design document, in-code documentation.
- All necessary methods (no getter, setter, or constructor) have a documentation.

## Issue Closed by this pull request:

This PR closes #<issue number>
