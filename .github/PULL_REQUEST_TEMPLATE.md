# Technical Checklist
## What is the purpose of the change
*(For example: This pull request makes task deployment go through the gRPC server, rather than through CAF. That way we avoid CAF's problems them on each deployment.)*

## Brief change log
*(for example:)**
  - *The TaskInfo is stored in the RPC message*
  - *Deployments RPC transmits the following information:....*
  - *NodeEngine does the following: ...*

## Issue(s) closed by this pull request
This PR closes #<issue number>.

## Verifying this change
This change is tested enough, either by unit tests, integration tests, or manual tests (via a script).
- [ ] PR assignee
- [ ] Reviewer 1
- [ ] Reviewer 2
- [ ] Reviewer 3 (Optional)

## Does this pull request potentially affect one of the following parts:
*(remove inapplicable items. Feel free to state if you are unsure about potential impact.)*
  - Dependencies (does it add or upgrade a dependency): (yes / no)
  - The compiler: (yes / no / don't know)
  - The threading model: (yes / no / don't know)
  - The Runtime per-record code paths (performance sensitive): (yes / no / don't know)
  - The network stack: (yes / no / don't know)
  - Anything that affects deployment or recovery: Coordinator (and its components), NodeEngine (and its components): (yes / no / don't know)

- [ ] PR assignee
- [ ] Reviewer 1
- [ ] Reviewer 2
- [ ] Reviewer 3 (Optional)
# Non-Technical Checklist
### Are all issue numbers included and the PR is not added to any project or milestone?

- [ ] PR assignee
- [ ] Reviewer 1
- [ ] Reviewer 2
- [ ] Reviewer 3 (Optional)

### Are the commits atomic and well-organized?

- [ ] PR assignee
- [ ] Reviewer 1
- [ ] Reviewer 2
- [ ] Reviewer 3 (Optional)

### Does there exist good enough documentation both in the code and a markdown for the bigger component?

- [ ] PR assignee
- [ ] Reviewer 1
- [ ] Reviewer 2
- [ ] Reviewer 3 (Optional)

### Is the documentation free of grammar and spelling mistakes?

| PR assignee             | Reviewer 1              | Reviewer 2              | Reviewer 3 (Optional)   |
|-------------------------|-------------------------|-------------------------|-------------------------|
| <ul><li>-[ ] </li></ul> | <ul><li>-[ ] </li></ul> | <ul><li>-[ ] </li></ul> | <ul><li>-[ ] </li></ul> |
