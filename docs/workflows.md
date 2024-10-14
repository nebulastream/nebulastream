## Compile-Time-Regression

The compile time regression workflow measures the compilation time every
night. It is intended to track the slowly increasing compilation time and
allows for analyzing pending PRs before merging them.

It can be invoked via the gh CLI. Local support via `act` is currently not implemented.

```
gh workflow run .github/workflows/compile-time-regression.yml --field branch="your-branch"
```
