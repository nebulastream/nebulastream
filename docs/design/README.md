# What is the Purpose of a Design Document
We create design documents for problems that require changes that are too complex or severe to be handled in a simple issue and that require in-depth analysis of the problem and synchronization with the team.
The review process of a design document can be viewed as a synchronization phase where every team member can asynchronously discuss the design document until a consensus is reached. 
When [the design document has been accepted](#how-is-a-design-document-accepted), the synchronization point is reached and we merge the design document into main. 

Design documents live in our codebase and record major problems that we faced, what our goals were when dealing with these problems, to which degree our solution reached that these goals, and which alternatives to our solution we considered and why we did not choose them. In contrast to technical documentation, which only describes the final state of our design decisions, design documents document the path that lead to making the specific design decisions. As a result, design documents explain the rationale behind our design decisions.

# When to Create a Design Document
## Github Discussions
We use Github discussions for drafting designs with whoever wants to participate in the discussion. In contrast to a design document, a Github discussion can be used to brainstorm ideas and does not need to be formal.

## Design Documents
Design documents organically grow out of Github discussions that reached a conclusion. A conclusion is reached if it is possible to formalize at least the [The Problem](#the-problem), the [Goals](#goals), the [Non-Goals](#non-goals), the [Proposed Solution](#proposed-solution), and the [Alternatives](#alternatives) sections of the design document. When creating the design document, the Github discussion should be condensed to contain only the relevant information. Additionally, the content of a potentially colloquial discussion needs to be formalized. 

Since a design document claims time from multiple people from our team, the following should be considered before creating a design document:
- A design document should be written with a reader-first mindset. Carefully consider if the design document is worth the time of other team members. Carefully consider if it is worth your time.
- We strongly encourage to create a Github discussion first to discuss the underlying problem and to get valuable feedback from the team.

# The Process
1. The conditions meet the criteria set in [When to Create a Design Document](#when-to-create-a-design-document).
2. Create an issue for the design document from the related Github Discussion. Use the design document tag ([DD]) for the issue.
3. Use the `create_a_new_design_document.sh` shell script to create a new design document.
4. Create a PR for the issue of the design document.
5. Assign all maintainers as reviewers to the PR of the design document.
6. Collect feedback from the reviewers.
7. Address the feedback of the reviewers.
8. If possible, and not already done, create a [Minimal Viable Prototype](#minimal-viable-prototype) for the [Proposed Solution](#proposed-solution)
9. The design document is accepted, if two code owners and one additional maintainer accept the PR. 
10. Ask one of the code owners to merge the PR.

# Design Document Template
Below we discuss the different sections of the design document template.

## The Problem
The problem section of the design document should explain the current state of our system and explain why we require a change that warrants this design document. To this end, the section should contain the context necessary to understand the problem(s). A concise description of the problem(s). If there are multiple problems, enumerate them as P1, P2, etc., so that they can be referenced distinctly.

## Goals
The design document should list all the explicit goals of the proposed change. By achieving all goals, all problems stated in [The Problem](#the-problem) should be mitigated. This section should not be verbose, but instead precisely state what the goals are. It might be a good idea to write this section before performing deeper research or implementing a prototype. The goals can also be seen as requirements to pass a proposed solution and the proposed solution should address the defined goals. If there are multiple goals, enumerate them as G1, G2, etc., so that they can be referenced distinctly. For each goal, mention which problem of [The Problem](#the-problem) it addresses and how it addresses it.

## Non-Goals
This section should list everything that is related to the design document, but is out of scope of the design document. Reason for every non-goal, why it is not a goal of the design document.

## (Optional) Solution Background
If the proposed solution builds up on prior work (issues, PRs), by the author of the design document or by colleagues, or if it strongly builds on top of designs used by other (open source) projects, then a solution background chapter can provide the necessary link to the prior work, without interfering with the description of the actual solution.
If the prior work is not discussed in detail in the [Alternatives](#alternatives) section, then the advantages and disadvantages of the prior work need to be discussed. It is a good idea to somehow label different prior work so that people know how to reference it in the PR discussion.

## Proposed solution
If possible, the section should start with a high level overview of the solution. This can entail a [system context diagram](https://en.wikipedia.org/wiki/System_context_diagram) that explains the interfaces of a (new) component of the system in relation to other components of the system. Another good option are mermaid diagrams, e.g., a class diagram representing a potential implementation of the proposed solution.
Furthermore, addressing the individual [Goals](#goals) and showing why the proposed solution achieves the individual goals and thereby overcomes ["The Problem"](#the-problem) helps structuring this section.

## Minimal Viable Prototype
If possible, a minimal viable prototype should be provided that demonstrates that the solution generally works. A prototype might be created after creating a [Draft](#draft)

## Alternatives
In this section, alternatives to the proposed solution need to be discussed. Each alternative should be tagged, e.g., A1, A2, and A3, to enable precise and consistent references during PR discussions.
It should be clear what the advantages and the disadvantages of each alternative it must be clear why the proposed solution was preferred over the respective alternative. If possible, argue why the alternatives do not achieve certain [Goals](#goals) in contrast to [Proposed solution](#proposed-solution).

## (Optional) Open Questions
All questions that are relevant for the design document, but that cannot and do not need to be answered before merging the design document. We might create issues from these questions or revisit them regularly.

## (Optional) Sources and Further Reading
If possible, the design document should provide links to resources that cover the topic of the design document. This has three advantages. First, it shows that the author of the design document performed research on the topic. Second, it provides additional reading material for reviewers to better understand the topic covered by the design document. Third, it provides more in-depth documentation for future readers of the design document. All sources that went into the process of creating the design documents should not be lost.

## (Optional) Appendix
Implementation details or other material that would otherwise disturb the reading flow of the design document should be placed in an appendix. Thereby, interested readers can still study the details or other documents if they wish to do so, but other readers are not distracted.

# Sources for this Design Document Template Document
- [Design documents at Google](https://www.industrialempathy.com/posts/design-docs-at-google/)
- [Design documents at Materialized](https://github.com/MaterializeInc/materialize/tree/main/doc/developer/design)