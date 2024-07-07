# The Problem
The current nautilus query compiler has some limitations that make it difficult to implement certain operators. 
One of the limitations is that it does not support the use of the standard library.
Another one is that it does not support the use of primitive C++ data types.
Finally, by extending the new library with std::tuple we can return multiple values from a function, which is not possible with the current Nautilus query compiler.


# Goals and Non-Goals
We would like to replace the existing Nautilus query compiler with the [NebulaStream library](https://github.com/nebulastream/nautilus/).
The maintainer of the NebulaStream library is Philipp Grulich (EX-DIMA), and he has agreed to help us with the integration as well as with the implementation of any missing components.
We do not want to discuss the use of other query compilers at this time.

[//]: # (# &#40;Optional&#41; Solution Background)

# Our Proposed Solution
In our proposed solution, we have to complete the following steps. 
Some (sub-)steps can be done in parallel, but some (sub-)steps depend on the completion of other steps.

## Integrate the NebulaStream library into the NebulaStream project.
After this step, we should be able to use the NebulaStream library in our project.
This means that we can include the library in our project and use the library's classes and functions.
We plan on including the NebulaStream library via vcpkg in our project..
To make the removal of the old Nautilus library easier, we will include the NebulaStream library in `nes-nautilus-new/include` for the short term.
We will create a folder `nes-nautilus-new/include/Interface`, where we will place all the missing data classes,  e.g., `Record`, `RecordBuffer`, `MemoryProvider`, etc.


## Implement missing data classes necessary for our current interface
In our current interface, we have some data classes that are not yet implemented in the NebulaStream library.
Moreover, the nautilus library plans on being a general data processing framework.
Therefore, we have to implement at least the following classes under `nes-nautilus-new/include/Interface`:
- `Record`
- `RecordBuffer`
- `MemoryProvider`
- [`ExecutableDataType`](https://github.com/nebulastream/nemo/blob/main/src/operators/DataTypes.hpp)

Once we have implemented these classes, we plan on creating a simple filter operator that uses these classes to test if the implementation is correct.
To this end, we will create a new folder `nes-execution-new` where we will place the new operator.


## Port the existing operators to the new NebulaStream library.
After, we have implemented the missing data classes, we can start porting the existing operators to the new NebulaStream library.
This should be quite straightforward, as we have to solely change the syntax but not think about how to implement the operators.
We will place the new operators in the folder `nes-execution-new` and remove the old operators in the folder `nes-execution`.
This way, we can easily gauge the progress of the porting process.


## Replace the existing compilation call with the NebulaStream library's compilation call
In this step, we have to replace the existing compilation call with the NebulaStream library's compilation call.
We therefore have to replace the call to the old Nautilus library with the call to the new NebulaStream library.
This step should be quite straightforward, as we have already implemented the missing data classes and ported the existing operators to the new NebulaStream library.
Once we are done with the replacement, we can also remove the old Nautilus files from our project.
A lot of them will be located under `nes-nautilus`, so it should be easy to remove them.


## Sequence Diagram of the Steps
To have a quick overview what steps depend on each other, we have created a diagram that shows in what order the steps should be completed.
```mermaid
graph TD;
    A[Integrate new library] --> B[Implement missing data classes necessary]
    B --> C[Port expressions]
    B --> D[Port operators]
    C --> E
    D --> E[Replace compilation call]
```

# Alternatives
One alternative approach is to nuke the current Nautilus query compiler and start from scratch directly on the main.
This is possible as a lot of the heavy-lifting has already been done in the branch `feature/25-nautilus-lib-integration`.
We can directly take all steps and have a small selection of operators, e.g., filter and a map with 5-8 expressions, that we can use to test the new library.
Then we can start implementing the missing operators and expressions in the new library.
For the expressions, it is simply porting the expressions to the new library.
For the joins, they require a bit more time as we would have to refactor them.
For the aggregations, they require some more time as we would have to provide some documentation in terms of inline comments for them.




# Open Questions
- Do we wait until we have the plugin registry to implement the operators as plugins?
- Do we wait until we have system level tests to implement the operators with the new tests in mind?

# (Optional) Sources and Further Reading
- https://github.com/nebulastream/nautilus/
- https://github.com/nebulastream/nemo/
- https://github.com/nebulastream/nemo/blob/main/src/operators/DataTypes.hpp
- https://github.com/nebulastream/nebulastream-public/tree/feature/25-nautilus-lib-integration

[//]: # (# &#40;Optional&#41; Appendix)
