# What is the Purpose of a Design Document
We create design documents for problems that require changes that are too complex or severe to be handled in a simple issue, requiring in-depth analysis and synchronization with the team.
In essence, the design document review process is a synchronization phase during which every team member can asynchronously discuss the document until we reach a consensus.
When [we accept the design document](#how-is-a-design-document-accepted), we reach the synchronization point and merge the design document into the main branch.

Design documents live in our codebase and record significant problems that we faced, our goals when dealing with these problems, the degree to which our solution reached these goals, the alternatives to our solution we considered, and why we did not choose them. In contrast to technical documentation, which only describes the final state of our design decisions, design documents document the path that leads to making the specific design decisions. As a result, design documents explain the rationale behind our design decisions.

# When to Create a Design Document
## Github Discussions
We use Github discussions to draft designs with whoever wants to participate in the discussion. In contrast to a design document, a Github discussion can be used to brainstorm ideas and does not need to be formal.

## Design Documents
Design documents organically grow out of Github discussions that conclude.\
A discussion concludes if all participants of the discussion agree on a proposed solution.\
The participants of the discussion then decide whether the proposed solution is complex enough to warrant a design document.

To create the first version of a design document, we must be able to formalize at least [The Problem](#the-problem), the [Goals](#goals), the [Non-Goals](#non-goals), the [Proposed Solution](#proposed-solution), and the [Alternatives](#alternatives) sections of the design document.\
When creating the design document, the GitHub discussion should be condensed to contain only the relevant information. Additionally, we formalize the content of potentially colloquial discussions.

Since a design document claims time from multiple people from our team, we propose to consider the following before creating a design document:
- A design document should be written with a reader-first mindset. Carefully consider if the design document is worth the time of other team members. Carefully consider if it is worth your time.
- We strongly encourage creating a GitHub discussion first to discuss the underlying problem and to get valuable feedback from the team.

# The Process
1. The conditions meet the criteria in [When to Create a Design Document](#when-to-create-a-design-document).
2. Create an issue for the design document from the related Github Discussion. Use the design document tag ([DD]) for the issue.
3. Use the `create_a_new_design_document.sh` shell script to create a new design document.
4. Create a PR for the issue of the design document.
5. Assign all maintainers as reviewers to the PR of the design document.
6. Collect feedback from the reviewers.
7. Address the feedback of the reviewers.
8. If possible, and not already done, create a [Proof Of Concept](#proof-of-concept) for the [Proposed Solution](#proposed-solution)
9. The design document is accepted if two code owners and one additional maintainer accept the PR.
10. Ask one of the code owners to merge the PR.

# Design Document Template
Below, we discuss the different sections of the design document template.

## The Problem
The problem section of the design document should explain the current state of our system and why we require a change that warrants this design document. To this end, the section should contain the context necessary to understand the problem(s) and a concise description of the problem(s). If there are multiple problems, enumerate them as P1, P2, ..., so we can reference them distinctly.

## Goals
This section should precisely state all goals. Furthermore, the goals should address all problems stated in [The Problem](#the-problem). We suggest writing this section before performing more profound research or implementing a proof of concept. Another way to see goals is that goals are requirements that a proposed solution must fulfill. If multiple goals exist, enumerate them as G1, G2, ..., so we can reference them distinctly. For each goal, mention which problem(s) of [The Problem](#the-problem) it addresses and how it addresses it/them.

## Non-Goals
This section should list everything related to the design document that is out of the scope of the design document. For every non-goal, explain why it is out of the scope of the design document.
Enumerate non-goals as NG1, NG2, ..., so we can reference them distinctly.

## Alternatives
In this section, we discuss alternatives to the proposed solution.
We enumerate each alternative, e.g., A1, A2, and A3, to enable precise and consistent references during PR discussions.
We discuss the advantages and disadvantages of each alternative.
It must be clear why the proposed solution was preferred over the alternatives.
If possible, we argue why the alternatives do not achieve specific [Goals](#goals).
Towards the end of this section, we narrow down the alternatives to one, which we discuss in detail in the section [Proposed solution](#proposed-solution). 

## (Optional) Solution Background
If the proposed solution builds upon prior work, such as issues and PRs, by the author of the design document or by colleagues, or if it strongly builds on top of designs used by other (open source) projects, then a solution background chapter can provide the necessary link to the prior work, without interfering with the description of the actual solution.
It is a good idea to label different prior work so that people know how to reference it in the PR discussion.

## Proposed solution
The section should start with a high-level overview of the solution. This can entail a [system context diagram](https://en.wikipedia.org/wiki/System_context_diagram) that explains the interfaces of a (new) system component in relation to other system components. Another good option is mermaid diagrams, e.g., a class diagram representing a potential implementation of the proposed solution.
Furthermore, addressing the individual [Goals](#goals) and showing why the proposed solution achieves the individual goals and thereby overcomes ["The Problem"](#the-problem) helps structure this section.

## Proof Of Concept
A proof of concept (PoC) should demonstrate that the solution generally works. It is acceptable to create a PoC after creating an initial [Draft](#draft) for the design document.

# Summary
The main goal is to summarize, for the reviewer, what the problems and goals of the design document are and how the proposed solution addresses all [problems](#the-problem) and achieves all [goals](#goals) and if it does not, why that is ok.
Additionally, we briefly state why the proposed solution is the best [alternative](#alternatives).

## (Optional) Open Questions
All questions that are relevant for the design document but that cannot and do not need to be answered before merging the design document. We might create issues from these questions or revisit them regularly.

## (Optional) Sources and Further Reading
The design document should provide links to resources that cover the topic. This has three advantages:
1. It shows that the author of the design document performed research on the topic.
2. It provides additional reading material for reviewers to better understand the topic covered by the design document. 
3. It provides more in-depth documentation for future readers of the design document.
We should keep the sources that went into creating the design documents.

## (Optional) Appendix
Implementation details or other material that would otherwise disturb the reading flow of the design document should be placed in an appendix. Thereby, interested readers can still study the details or other documents if they wish to do so, but other readers are not distracted.

# Sources for this Design Document Template Document
- [Design documents at Google](https://www.industrialempathy.com/posts/design-docs-at-google/)
- [Design documents at Materialized](https://github.com/MaterializeInc/materialize/tree/main/doc/developer/design)
