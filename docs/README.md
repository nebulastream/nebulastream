# What is the Purpose of a Design Document
We create design documents for problems that require changes, which are too complex or severe to be handled in a simple issue and that require in-depth analysis of the problem and synchronization with the team.
The review process of a design document can be viewed as a synchronization phase where every team member can asynchronously discuss the design document until a consensus is reached. When most people agree,the synchronization point is reached and we merge the design document into main.
Furthermore, design documents are valuable documentation that preserve our reasoning for introducing specific design changes. Design documents live in our codebase and record major problems that we faced, what our goals were when dealing with these problems, to which degree our solution reached that these goals, and which alternatives to our solution we considered and why we did not choose them.

# When to Create a Design Document
Typically, design documents organically grow out of Github discussions when we reach a conclusion. However, it is also ok to create a design document without a prior Github discussion if applicable.
Since a design document claims time from multiple people from our team, the following should be considered before creating a design document:
- A design document should be written with a reader-first mindset. Carefully consider if the design document is worth the time of other team members. Carefully consider if it is worth your time.
- If possible, create a Github discussion first to discuss the underlying problem and to get valuable feedback from the team.

# Process of Creating a Design Document
0. If feasible, start a Github discussion before creating the design document.
1. Copy the design document template in the docs folder. (Every design document must be represented as a Markdown file in the `docs` folder of the root directory.)
2. Rename the new copied design document template using the following schema: `YYYYMMDD_NAME_OF_DESIGN_THE_DOCUMENT.md`. (For example, a design document created on the 15th of January, 1929, would be called `19290115_NAME_OF_THE_DESIGN_DOCUMENT.md`.)
3. Create an issue for the design document and use the design document tag ([DD]) for the issue. If a design document is linked to a Github discussion, the issue for the design document should be created from the github discussion to provide a link between the discussion and the design document.
4. Create a PR for the design document and start a review process.

*Open question: do we enforce a specific structure in the docs directory?*

# Design Document Template
Below we discuss the different sections of the design document template.

## The Problem
The problem section of the design document should explain the current state of our system and why we require a change that warrants this design document. To this end, the context, motivation and problems of the current state should be addressed. The main goal is to precisely state what the problem targeted by the design document is. A sub-goal is to provide context, align and motivate all readers of the design document.

## Goals and Non-Goals
The design document should list all the explicit goals of the proposed change. This section should not be verbose, but instead precisely state what the goals are and what the non-goals are. It might be a good idea to write this section before performing deeper research or implementing a prototype. The goals can also be seen as requirements to pass a proposed solution and the proposed solution should address the defined goals.

## (Optional) Solution Background
If the proposed solution builds up on prior work (issues, PRs), by the author of the design document or by colleagues, or if it strongly builds on top of designs used by other (open source) projects, then a solution background chapter can provide the necessary link to the prior work, without interfering with the description of the actual solution.
If the prior work is not discussed in detail in the [Alternatives](#alternatives) section, then the advantages and disadvantages of the prior work need to be discussed. It is a good idea to somehow label different prior work so that people know how to reference it in the PR discussion.

## Our Proposed Solution
If possible, the section should start with a high level overview of the solution. This can entail a [system context diagram](https://en.wikipedia.org/wiki/System_context_diagram) that explains the interfaces of a (new) component of the system in relation to other components of the system. Another good option are mermaid diagrams, e.g., a class diagram representing a potential implementation of the proposed solution.

It is not necessary to provide a fully detailed technical solution. The solution only needs be described to a level of detail that the reader can imagine a technical solution to the problem. If possible, a prototype should be provided that demonstrates that the solution generally works (*open question: Materialized has a dedicated section for a prototype, should we follow that design?*).

## Alternatives
In this section, alternatives to the proposed solution need to be discussed. Each alternative should be tagged, e.g., A1, A2, and A3, to enable precise and consistent references during PR discussions.
It should be clear what the advantages and the disadvantages of each alternative it must be clear why the proposed solution was preferred over the respective alternative.

## (Optional) Sources and Further Reading
If possible, the design document should provide links to resources that cover the topic of the design document. This has three advantages. First, it shows that the author of the design document performed research on the topic. Second, it provides additional reading material for reviewers to better understand the topic covered by the design document. Third, it provides more in-depth documentation for future readers of the design document. All sources that went into the process of creating the design documents should not be lost.

### Source for this Design Document Template Document
- [Design documents at Google](https://www.industrialempathy.com/posts/design-docs-at-google/)
- [Design documents at Materialized](https://github.com/MaterializeInc/materialize/tree/main/doc/developer/design)

## (Optional) Appendix
Implementation details or other material that would otherwise disturb the reading flow of the design document should be placed in an appendix. Thereby, interested readers can still study the details or other documents if they wish to do so, but other readers are not distracted.