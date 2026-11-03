# GitHub Issue Structure

We organize work as an **issue tree** using GitHub sub-issues. Every issue has a **type** (its size) and **labels** (its scope).

## Issue Types

| Type | Role in the tree | Rules |
|---|---|---|
| **EPIC** | Root | Must have **at least 2** sub-issues (FEATUREs or TASKs). |
| **FEATURE** | Internal node or root | Must have **at least 2** sub-issues (TASKs only). |
| **TASK** | Leaf | Has no sub-issues. |

- **EPIC**: a large, long-running initiative with a strategic goal, usually spanning several components or areas of the system. Has its own goals and start/target dates; closed when all goals are met.
- **FEATURE**: one self-contained capability delivered over several PRs. Its scope is a single coherent deliverable, not a broad initiative, requiring more than one PRs.
- **TASK**: a single unit of work, closed by one PR (e.g. a bug fix or a refactoring step).

**EPIC vs. FEATURE:** a FEATURE answers *"what capability do we ship?"* and its children are only PR-sized TASKs.
An EPIC answers *"what larger goal are we pursuing?"* and is usually made of several FEATUREs.
If the work splits into deliverables that are worth tracking on their own, it is an EPIC; if it only splits into PR-sized steps, it is a FEATURE.

``` 
# Example EPIC tree
EPIC
├── FEATURE
│   ├── TASK
│   └── TASK
└── TASK

# Example FEATURE tree
FEATURE (orphan)
├── TASK
└── TASK

# Example orphan TASK
TASK (orphan)
```

## Orphans

An **orphan** is an issue with no parent.

- A **FEATURE** can be an orphan (e.g. an issue with several TASKs but not big enough to be considered EPIC).
- A **TASK** can be an orphan (a standalone piece of work like a bug fix).
- An **EPIC** is always a root (thus an orphan), so it is unnecessary to label it as orphan.

## Labels

Use **labels** to categorize issues by **scope** (e.g. engine, optimizer, udf). Labels are independent of type. 
Labels should mostly be used in TASKs, as EPICs and FEATUREs might require too many labels to completely cover their scope.
Overall, the labels should be used in a high-level, metadata-type way to quickly refer to the scope of the issue.

## How to Create Issues

1. **Pick the type by size.**
   - Large initiative with multiple parts → **EPIC**
   - Self-contained deliverable made of several tasks → **FEATURE**
   - Single unit of work → **TASK**
2. **Link it to its parent** as a sub-issue, unless it's an EPIC or an intentional orphan.
3. **Check the minimum children.**
   - EPIC: 2+ sub-issues
   - FEATURE: 2+ TASKs
   - If it can't be split into 2+ pieces, make it a smaller type.
4. **Add scope labels.**
   - Label as orphan if necessary. 

## Planning

- Set **priority** on every issue regardless of type.
- Set **target dates on EPICs and FEATUREs**, not on every TASK.
- Use **GitHub Projects** views to filter and group by type, label, and parent.

## Disclaimer

The EPIC/FEATURE/TASK templates do not set the issue type; maintainers set it manually after creating the issue.
External contributors cannot set issue types and should use the other issue templates instead.
