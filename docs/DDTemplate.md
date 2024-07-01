# Context and Scope
A design document introduces a change to the system that is severe enough that it warrants a design document.
The context and scope section of the design document should discuss the following two aspects of the change:
1. Explain the current state and why we require a change that warrants this design document.
2. The scope of the change described in the design document. Which parts of the system does the change impact?

To address 1., background, motivation and problems with the current state should be addressed. The goal should be to align all readers of the design document.
To address 2., the change needs to be put into the context of the entire system. It needs to be clear which parts of the system are affected by the proposed change. If possibly, a diagram should be used to demonstrate the scope. For example, a [system context diagram](https://en.wikipedia.org/wiki/System_context_diagram) can be helpful.


# Goals and Non-Goals
The design document should list all the explicit goals of the proposed change. This section should not be verbose, but instead precisely state what the goals are and what the non-goals are. It might be a good idea to write this section before performing deeper research or implementing a prototype. The goals can also be seen as requirements to pass a proposed solution and the proposed solution should address the defined goals.

# (Optional) Solution Background
If the proposed solution builds up on prior work, by the author of the design document or by colleagues, or if it strongly builds on top of designs used by other (open source) projects, then a solution background chapter can provide the necessary link to the prior work, without interfering with the description of the actual solution.
If the prior work is not discussed in detail in the [Alternatives](#alternatives) section, then the advantages and disadvantages of the prior work need to be discussed. It is a good idea to somehow label different prior work so that people know how to reference it in the PR discussion.

# Our Proposed Solution
This section presents the proposed solution. Since the context, the scope, the goals and potentially the background for the solution were discussed before, this section can focus solely on the technicalities of the proposed solution.
If possible, the section should start with a high level overview, e.g., a mermaid diagram, of the proposed solution. In contrast to a system context diagram (mentioned in [Context and Scope](#context-and-scope)) this diagram should zoom in on the solution, blurring out the rest of the system as much as possible. Interfaces to other components should be discussed in [Context and Scope](#context-and-scope) as much as possible.

It is not necessary to provide a fully detailed technical solution. The solution only needs be described to a level of detail that the reader can imagine a technical solution to the problem. If possible, a prototype should be provided that demonstrates that the solution generally works.

# Alternatives
In this section, alternatives to the proposed solution need to be discussed. Each alternative should be tagged, e.g., A1, A2, and A3, to enable precise and consistent references during PR discussions.
It should be clear what the advantages and the disadvantages of each alternative it must be clear why the proposed solution was preferred over the respective alternative.


# (Optional) Sources and Further Reading
If possible, the design document should provide links to resources that cover the topic of the design document. This has three advantages. First, it shows that the author of the design document performed research on the topic. Second, it provides additional reading material for reviewers to better understand the topic covered by the design document. Third, it provides more in-depth documentation for future readers of the design document. All sources that went into the process of creating the design documents should not be lost.

## Source for this Design Document Template Document
https://www.industrialempathy.com/posts/design-docs-at-google/

# (Optional) Appendix
Implementation details or other material that would otherwise disturb the reading flow of the design document should be placed in an appendix. Thereby, interested readers can still study the details or other documents if they wish to do so, but other readers are not distracted.