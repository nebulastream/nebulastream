# Logical Operator Refactoring Summary

## Initial Goal

The primary objective was to refactor all logical operators to use a new Curiously Recurring Template Pattern (CRTP) base class, `LogicalOperatorHelper`. The new design aims to centralize shared logic, eliminate boilerplate code, and improve maintainability by having each operator define a simple `Data` struct for its specific fields.

## Work Completed

I have performed two major refactoring passes on the logical operators located in `nes-logical-operators/include/Operators/`.

### 1. Migration to CRTP Base Class (`LogicalOperatorHelper`)

I migrated the following logical operators from the old `LogicalOperatorConcept` to the new `LogicalOperatorHelper<T>` design:

-   `ProjectionLogicalOperator`
-   `UnionLogicalOperator`
-   `IngestionTimeWatermarkAssignerLogicalOperator`
-   `EventTimeWatermarkAssignerLogicalOperator`
-   `SinkLogicalOperator`
-   `SourceDescriptorLogicalOperator`
-   `SourceNameLogicalOperator`
-   `JoinLogicalOperator`
-   `WindowedAggregationLogicalOperator`

The migration process for each operator involved:
-   Changing the base class to `LogicalOperatorHelper<DerivedOperator>`.
-   Creating a nested `struct Data` to hold all operator-specific member variables.
-   Removing all boilerplate methods (e.g., `operator==`, `serialize`, `withChildren`, `getTraitSet`, etc.) that are now handled by the `LogicalOperatorHelper` base class.
-   Adding public getter methods for all fields within the `Data` struct to ensure proper encapsulation.
-   Making `LogicalOperatorHelper` a `friend` of the operator class to allow the base class to access the private `data` member for serialization and equality checks.
-   Updating the corresponding `.cpp` implementation files and the `LowerToPhysical` rewrite rules to use the new getters instead of accessing data members directly.

### 2. Unifying Input Schema Handling

Following the initial migration, I performed a second refactoring to standardize how input schemas are handled:

-   Modified the `LogicalOperatorHelper` base class to store a `std::vector<Schema> inputSchemas` instead of a single `Schema inputSchema`.
-   Updated all the aforementioned logical operators to correctly interact with the vector of schemas, particularly in their `withInferredSchema` methods.
-   This change allowed for the removal of redundant schema management fields in operators that handle multiple inputs, such as `UnionLogicalOperator` and `JoinLogicalOperator`, simplifying their implementations.

## Current Problem: Compilation Errors

After these refactoring steps, the project fails to compile. The errors can be categorized as follows:

1.  **Serialization Errors (Primary Blocker)**:
    -   The majority of errors originate from the `rfl::flexbuf::write` function within the `LogicalOperatorHelper::serialize` method.
    -   The serialization library does not have native support for several custom types used within the `Data` structs, including:
        -   `NES::FieldIdentifier`
        -   `NES::Windowing::TimeUnit`
        -   `NES::WindowMetaData`
        -   `NES::OriginIdAssignerTrait`
        -   Shared pointers to custom types (e.g., `std::shared_ptr<Windowing::WindowType>`).
    -   **Attempted Fix**: I tried to add custom `rfl::parsing::Parser` specializations for these types. This was an incorrect approach. The right solution likely involves implementing custom `to_flexbuf` and `from_flexbuf` functions or providing a `rfl::Reflector` specialization for each unsupported type to guide the serialization library.

2.  **`OperatorSerializationUtil.cpp` Error**:
    -   There is an error in the `deserializeOperator` function when handling a `SinkLogicalOperator`. The code attempts to directly access the `data` member of the `sinkOperator` instance (`sinkOperator.data.sinkDescriptor = ...`), which is private.
    -   **Required Fix**: This should be corrected by using the appropriate public method, likely `withSinkDescriptor()`.

3.  **`LogicalPlanTest.cpp` Errors**:
    -   A helper struct, `TestTrait`, was defined locally within a test function (`TEST_F(LogicalPlanTest, AddTraits)`). This is not allowed for structs containing `static constexpr` members.
    -   The `withTraitSet` function was being called with an incorrect type.
    -   **Required Fix**: The `TestTrait` struct definition needs to be moved outside the test function to the top level of the file. The call to `withTraitSet` needs to be corrected to pass a `TraitSet` object.

4.  **`LowerToPhysicalNLJoin.cpp` Syntax Error**:
    -   A syntax error was introduced that resulted in a redefinition of the `probeOperator` variable.
    -   **Required Fix**: The erroneous line needs to be corrected to properly initialize the variable in a single statement.

The serialization issues are the most significant blocker and will require a more in-depth solution to instruct the `rfl::flexbuf` library on how to handle the custom data structures. The other errors are more straightforward syntax and design issues that I was in the process of fixing.