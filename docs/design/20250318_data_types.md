# The Problem

- describe the current state of the system
- explain why the system's current state requires a change
- specify the concrete problems and enumerate them as P1, P2, ...

# Goals
## G1 - Simplicity
Simplicity should always be a goal, but given that data types are relevant on all levels of our system, it is especially important in this case.
## G2 - Extensibility
Given that extensibility is a general goal of NebulaStream and that data types our user-facing, we must allow users to create custom data types.
## G3 - Composable
We could implement a version of our data types that is simple (G1) and extensible (G2), but that requires users to essentially reimplement a significant part of the logic of data types for every new (custom) data type.
Instead, our data types must be composable.
When implementing a new data type, a user should be able to rely on prior implementations of data types to compose the new data type. 
 ## G4 - Distributed
Coordinator/Worker
Global/Local optimizer
Different backends
What if one worker node has a plugin (data type), but another has not?

# Non-Goals
- list what is out of scope and why
- enumerate non-goals as NG1, NG2, ...

# Alternatives
## A1 - Single Implementation
Other (interpreting) systems (DuckDB, ClickHouse) have one implementation for their data types. 
There is no explicit difference between 'logical' and 'physical' data types.
A data type is enriched with physical information.
Furthermore, there is a physical layout, i.e., the way the data is represented in-memory (/ on-disk).
### Advantage
This approach simplifies implementation. 
There is one place that holds all information regarding data types.
Implementing a new data type means extending (G2) a single interface.c
### Problems
As long as we use Nautilus as our primary backend, we must support the 'Value' type of Nautilus, as Nautilus essentially is 'Value'.
Thus, Nautilus would be our single implementation.
This would mean that the frontend needs to know about Nautilus.
Also, tracing and other Nautilus details would be part of our general data types.
Lastly, lowering to another backend would involve an awkward process of transforming Nautilus values to other data types.
### Conclusion
We need at least 2 implementations of our data types.
First, an implementation that the front-end uses.
Second, the Nautilus implementation.
Thus, it is not possible to follow a `pure` single implementation approach.

## A2 - Logical, Physical, and Nautilus
In the current version of NebulaStream, we have three levels of data types (ignoring nautilus-backend-specific data types).
Logical data types, that represent a general data type, such as `Integer`, without specifying the type (of Integer) yet.
A physical data type that represents the precise data type that our we want to target with our backends, e.g., UINT_32.
A Nautilus data type that we map our physical data type to.
### Advantage
The physical data type allows us to swap out a physical implementation, e.g., Nautilus without changing the implementation of the datatypes.
### Problems
We need to maintain two levels of data types, logical and physical.
Every logical data type must map to physical data type.
Manipulating data types (changing/extending) requires to implement the logical, physcial and nautilus representation (and potentially optimization/lowering rules).
### Conclusion
Instead of implementing and maintaining separate logical and physical representation of data types, we can extend the logical representation with physical information.
This is an approach that DuckDB and ClickHouse follow.
Following the proposal by @adrianmichalke in the PoC for the operator representations, logical data types could have traits.
The traits represent specifications of the data types derived by the optimizer.
In the final step, when the optimizer lowers the query plan to a physical query plan, the optimizer transforms the logical data types with the accumulated traits to a physical representation of the data types.

### A3 - Logical, Nautilus
Given the downsides of a single implementation for data types in the context of NebulaStream (A1) and the downsides of having a dedicated physical implementation (A2), an in-between solution seems logical.
### A3.1 Minimal Logical Implementation
The logical implementation could be a single struct/class that represents all data types (logically).
The (physical) details could be traits/properties.
#### Advantage
Extremely simple (G1) implementation that supports the current version of NebulaStream.
Simple extensibility (G2), using traits. 
In essence, a new data type would construct the DataType with a different initial set of traits.
#### Disadvantage
All details of data types would be hidden behind traits, which makes it difficult to understand which data types the system supports.
If data types are just 'traits/properties' of 'DataType', then we don't get any support from the type system.
We could use enums to represent the data types and use the enums, e.g., for type inference.
However, extending the set of enums, even at compile time, is not simple.

Point(1,1)

Point x;
Point y;
x.add(y);
add(Point p1, Point p2):
    Point(p1.x + p2.x, p1.y + p2.y)
    (NodeFunctionAdd(x.x, y.x), NodeFunctionAdd(x.y, y.y))
--> logical 'functions' must map to Executable

SELECT * FROM Points
WHERE Point(a,b) + Point(c,d) == Point(UINT64(12),UINT64(4))

Think about:
- optimizer rules
    - rewrite rules
- serialization
- implementation of logical data type (Basic/Complex)
- type inference on logical data type
    - operators
    - functions
    - think about Complex types
    - think about extended types
- lowering to physical
    - PhysicalMemoryLayout (on worker)
    - Nautilus Values
        - profit from inference logic / type rules (don't implement a second time)

Todo:
- Serialization
    - Idea: General seralization (user does not need to implement serialization logic)
        - DataType is either Basic or Complex
            - Basic:
                - There is a limited set of basic data types
                - all basic data types have serialization logic
            - Complex:
                - a complex data type consists of basic data types
                - to serialize a complex data type, we serialize all its basic data types, preserving order, etc.


Todo:
- Type Inference
    - logical level
    - Nautilus
    - Problem:
        - implement type inference for every function/operator for all combinations of data types?
        - implement twice? once for Nautilus, once for logical?
            - logical could impose interface that Nautilus MUST implement (guaranteed that infered operations are supported)
                - Preferrable
            - Nautilus implementations could determine logical inference
                - would hard-tie to Nautilus

Todo:
- nested types
    - nested type as default? <-- kind of like a grammar:
        -  ComplexType -> BasicType | (ComplexType, ComplexType) | (BasicType, ComplexType) | (ComplexType, BasicType)
        -  BasicType -> Int | Float | Char | Bool | Decimal .. 
    - examples:
        - Array: ComplexType(Int, BasicType)
        - VarSized: ComplexType(BasicType)
        - Decimal: ComplexType(Int, Int)
- could model as:
    DataType = variant<BasicType, ComplexType>
    BasicType = ...
    ComplexType = vector<DataType>
Details:
- enum representation
- variant representation
- classes representation


Problem: BasicTypes and NativeType <-- the same, except for sight naming differences and UNDEFINED (physical)
Problem: logical data types already contain physical information such as `bits`, `upper-/lowerBound`


# (Optional) Solution Background
- describe relevant prior work, e.g., issues, PRs, other projects

# Our Proposed Solution
- start with a high-level overview of the solution
- explain the interface and interaction with other system components
- explain a potential implementation
- explain how the goals G1, G2, ... are addressed
- use diagrams if reasonable

# Proof of Concept
- demonstrate that the solution should work
- can be done after the first draft

# Summary
- this section closes the Problems/Goals bracket opened at the beginning of the design document
- briefly recap how the proposed solution addresses all [problems](#the-problem) and achieves all [goals](#goals) and if it does not, why that is ok
- briefly state why the proposed solution is the best [alternative](#alternatives)

# (Optional) Open Questions
- list relevant questions that cannot or need not be answered before merging
- create issues if needed

# (Optional) Sources and Further Reading
- list links to resources that cover the topic

# (Optional) Appendix
- provide here nonessential information that could disturb the reading flow, e.g., implementation details