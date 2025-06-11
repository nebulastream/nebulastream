# Spatiotemporal Operations Integration Guide for NebulaStream

This guide provides step-by-step instructions for integrating spatiotemporal operations (filter, map, aggregation) with MEOS library into the NebulaStream system.

## Overview

MEOS (Mobility and Expansion of Open-Source) provides spatiotemporal data types and operations. To integrate it with NebulaStream, you'll need to modify multiple components across the architecture.

## Integration Points

### 1. External Dependencies and Build System

#### Step 1.1: Add MEOS to vcpkg dependencies
**Location**: `vcpkg.json` (if exists) or CMake find modules

```json
{
  "dependencies": [
    "meos",
    // ... existing dependencies
  ]
}
```

#### Step 1.2: Update main CMakeLists.txt
**Location**: `CMakeLists.txt` (root)

Add after the existing `find_package` calls:

```cmake
# Find MEOS library
find_package(MEOS REQUIRED)
if(MEOS_FOUND)
    message(STATUS "MEOS found: ${MEOS_VERSION}")
    set(MEOS_LIBRARIES ${MEOS_LIBRARIES})
    set(MEOS_INCLUDE_DIRS ${MEOS_INCLUDE_DIRS})
else()
    message(FATAL_ERROR "MEOS library not found")
endif()

# Add MEOS to global libraries
set(LIBRARIES ${LIBRARIES} ${MEOS_LIBRARIES})
```

#### Step 1.3: Create custom FindMEOS.cmake (if needed)
**Location**: `cmake/FindMEOS.cmake`

```cmake
# FindMEOS.cmake - Find MEOS library
find_path(MEOS_INCLUDE_DIR
    NAMES meos.h
    PATHS /usr/include /usr/local/include
    PATH_SUFFIXES meos
)

find_library(MEOS_LIBRARY
    NAMES meos libmeos
    PATHS /usr/lib /usr/local/lib
)

if(MEOS_INCLUDE_DIR AND MEOS_LIBRARY)
    set(MEOS_FOUND TRUE)
    set(MEOS_LIBRARIES ${MEOS_LIBRARY})
    set(MEOS_INCLUDE_DIRS ${MEOS_INCLUDE_DIR})
endif()
```

### 2. Data Types Extension

#### Step 2.1: Add spatiotemporal data types
**Location**: `nes-data-types/include/DataTypes/`

Create new files:
- `SpatioTemporalTypes.hpp`
- `MeosWrapper.hpp`

**File**: `nes-data-types/include/DataTypes/SpatioTemporalTypes.hpp`
```cpp
#pragma once
#include <NES/Common/DataTypes/DataType.hpp>
#include <meos.h>
#include <memory>

namespace NES::DataTypes {

class TemporalInstant : public DataType {
public:
    TemporalInstant();
    // MEOS temporal instant wrapper
private:
    std::shared_ptr<TInstant> meosInstant;
};

class TemporalSequence : public DataType {
public:
    TemporalSequence();
    // MEOS temporal sequence wrapper
private:
    std::shared_ptr<TSequence> meosSequence;
};

class SpatialPoint : public DataType {
public:
    SpatialPoint();
    // MEOS spatial point wrapper
private:
    std::shared_ptr<GSERIALIZED> geosPoint;
};

// Add other spatiotemporal types as needed
}
```

#### Step 2.2: Update data types CMakeLists.txt
**Location**: `nes-data-types/CMakeLists.txt`

```cmake
# Add MEOS include directories and libraries
target_include_directories(nes-data-types PUBLIC ${MEOS_INCLUDE_DIRS})
target_link_libraries(nes-data-types PUBLIC ${MEOS_LIBRARIES})
```

### 3. Logical Operators

#### Step 3.1: Create spatiotemporal logical operators
**Location**: `nes-logical-operators/include/Operators/LogicalOperators/`

Create files:
- `SpatioTemporalFilterLogicalOperator.hpp`
- `SpatioTemporalMapLogicalOperator.hpp`
- `SpatioTemporalAggregationLogicalOperator.hpp`

**File**: `nes-logical-operators/include/Operators/LogicalOperators/SpatioTemporalFilterLogicalOperator.hpp`
```cpp
#pragma once
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>
#include <Expressions/Expression.hpp>

namespace NES {

class SpatioTemporalFilterLogicalOperator : public LogicalUnaryOperator {
public:
    explicit SpatioTemporalFilterLogicalOperator(
        const ExpressionPtr& spatioTemporalPredicate,
        OperatorId id = 0
    );

    // Spatial predicates: contains, intersects, within, etc.
    // Temporal predicates: before, after, during, overlaps, etc.
    ExpressionPtr getSpatioTemporalPredicate() const;

    std::string toString() const override;
    LogicalOperatorPtr copy() override;
    
private:
    ExpressionPtr spatioTemporalPredicate;
};

using SpatioTemporalFilterLogicalOperatorPtr = std::shared_ptr<SpatioTemporalFilterLogicalOperator>;
}
```

**File**: `nes-logical-operators/include/Operators/LogicalOperators/SpatioTemporalMapLogicalOperator.hpp`
```cpp
#pragma once
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES {

class SpatioTemporalMapLogicalOperator : public LogicalUnaryOperator {
public:
    enum class SpatioTemporalFunction {
        TEMPORAL_POSITION,
        SPATIAL_CENTROID,
        TEMPORAL_DURATION,
        SPATIAL_BUFFER,
        TRAJECTORY_SPEED,
        DISTANCE_TRAVELED
    };

    explicit SpatioTemporalMapLogicalOperator(
        SpatioTemporalFunction function,
        const std::vector<ExpressionPtr>& parameters,
        OperatorId id = 0
    );

private:
    SpatioTemporalFunction function;
    std::vector<ExpressionPtr> parameters;
};
}
```

**File**: `nes-logical-operators/include/Operators/LogicalOperators/SpatioTemporalAggregationLogicalOperator.hpp`
```cpp
#pragma once
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES {

class SpatioTemporalAggregationLogicalOperator : public LogicalUnaryOperator {
public:
    enum class SpatioTemporalAggFunction {
        TEMPORAL_EXTENT,
        SPATIAL_UNION,
        TRAJECTORY_LENGTH,
        TEMPORAL_CENTROID,
        CONVEX_HULL
    };

    explicit SpatioTemporalAggregationLogicalOperator(
        SpatioTemporalAggFunction function,
        const std::vector<FieldAccessExpressionPtr>& groupByFields,
        OperatorId id = 0
    );

private:
    SpatioTemporalAggFunction function;
    std::vector<FieldAccessExpressionPtr> groupByFields;
};
}
```

### 4. Physical Operators

#### Step 4.1: Create physical operator implementations
**Location**: `nes-physical-operators/include/PhysicalOperators/`

Create files:
- `SpatioTemporalFilterPhysicalOperator.hpp`
- `SpatioTemporalMapPhysicalOperator.hpp`
- `SpatioTemporalAggregationPhysicalOperator.hpp`

**File**: `nes-physical-operators/include/PhysicalOperators/SpatioTemporalFilterPhysicalOperator.hpp`
```cpp
#pragma once
#include <PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <meos.h>

namespace NES::PhysicalOperators {

class SpatioTemporalFilterPhysicalOperator : public PhysicalUnaryOperator {
public:
    explicit SpatioTemporalFilterPhysicalOperator(
        const SchemaPtr& inputSchema,
        const SchemaPtr& outputSchema,
        const std::string& predicateExpression
    );

    ExecutionResult execute(TupleBuffer& inputBuffer, 
                          PipelineExecutionContext& pipelineExecutionContext,
                          WorkerContext& workerContext) override;

private:
    std::string predicateExpression;
    // MEOS predicate evaluation functions
    bool evaluateSpatialPredicate(const void* spatialData);
    bool evaluateTemporalPredicate(const void* temporalData);
};
}
```

#### Step 4.2: Implement physical operators
**Location**: `nes-physical-operators/src/PhysicalOperators/`

**File**: `nes-physical-operators/src/PhysicalOperators/SpatioTemporalFilterPhysicalOperator.cpp`
```cpp
#include <PhysicalOperators/SpatioTemporalFilterPhysicalOperator.hpp>
#include <meos.h>

namespace NES::PhysicalOperators {

SpatioTemporalFilterPhysicalOperator::SpatioTemporalFilterPhysicalOperator(
    const SchemaPtr& inputSchema,
    const SchemaPtr& outputSchema,
    const std::string& predicateExpression)
    : PhysicalUnaryOperator(inputSchema, outputSchema), 
      predicateExpression(predicateExpression) {
    
    // Initialize MEOS if not already done
    meos_initialize("UTC");
}

ExecutionResult SpatioTemporalFilterPhysicalOperator::execute(
    TupleBuffer& inputBuffer,
    PipelineExecutionContext& pipelineExecutionContext,
    WorkerContext& workerContext) {
    
    // Implementation using MEOS functions
    // Example: spatial containment, temporal overlap checks
    
    auto outputBuffer = workerContext.allocateTupleBuffer();
    
    for (uint64_t recordIndex = 0; recordIndex < inputBuffer.getNumberOfTuples(); ++recordIndex) {
        // Extract spatiotemporal data from input record
        // Apply MEOS predicates
        // If predicate passes, copy to output buffer
        
        bool predicateResult = evaluateSpatioTemporalPredicate(inputBuffer, recordIndex);
        if (predicateResult) {
            // Copy record to output
            outputBuffer.copyRecord(inputBuffer, recordIndex);
        }
    }
    
    return ExecutionResult::Ok;
}

bool SpatioTemporalFilterPhysicalOperator::evaluateSpatioTemporalPredicate(
    const TupleBuffer& buffer, uint64_t recordIndex) {
    
    // Use MEOS functions like:
    // - contains_bbox_geo_temporal()
    // - intersects_stbox_temporal()
    // - before_timestamp_temporal()
    // - overlaps_period_temporal()
    
    return true; // Placeholder
}
}
```

### 5. Query Compiler Integration

#### Step 5.1: Add code generation for spatiotemporal operations
**Location**: `nes-query-compiler/include/QueryCompilation/`

Update existing code generation to handle spatiotemporal types:

**File**: Add to existing code generator classes
```cpp
// In your code generation visitor pattern
class SpatioTemporalCodeGenerator {
public:
    std::string generateSpatialFilter(const SpatialFilterExpression& expr);
    std::string generateTemporalFilter(const TemporalFilterExpression& expr);
    std::string generateSpatioTemporalAggregation(const STAggregationExpression& expr);
    
private:
    // Generate calls to MEOS C functions
    std::string generateMeosCall(const std::string& functionName, 
                                const std::vector<std::string>& parameters);
};
```

### 6. Nautilus Integration

#### Step 6.1: Add MEOS function bindings to Nautilus
**Location**: `nes-nautilus/include/Nautilus/Interface/`

Create MEOS function bindings for JIT compilation:

**File**: `nes-nautilus/include/Nautilus/Interface/MeosFunctions.hpp`
```cpp
#pragma once
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Nautilus {

// Spatial predicate functions
FunctionCall spatialContains();
FunctionCall spatialIntersects();
FunctionCall spatialWithin();

// Temporal predicate functions  
FunctionCall temporalBefore();
FunctionCall temporalAfter();
FunctionCall temporalDuring();
FunctionCall temporalOverlaps();

// Aggregation functions
FunctionCall spatialUnion();
FunctionCall temporalExtent();
FunctionCall trajectoryLength();

}
```

### 7. SQL Parser Extension

#### Step 7.1: Add spatiotemporal SQL syntax
**Location**: `nes-sql-parser/`

Extend the SQL grammar to support spatiotemporal operations:

```sql
-- Spatial predicates
SELECT * FROM stream WHERE ST_Contains(geom_field, ST_Point(1.0, 2.0))
SELECT * FROM stream WHERE ST_Intersects(geom_field, region)

-- Temporal predicates  
SELECT * FROM stream WHERE T_Before(time_field, timestamp '2023-01-01')
SELECT * FROM stream WHERE T_During(time_field, period '[2023-01-01, 2023-12-31]')

-- Spatiotemporal aggregations
SELECT ST_Union(geom_field), T_Extent(time_field) 
FROM stream 
GROUP BY region_id
```

### 8. Testing

#### Step 8.1: Add unit tests
**Location**: `nes-*/tests/`

Create comprehensive tests for each component:

**Directories to add tests:**
- `nes-data-types/tests/SpatioTemporalTypesTest.cpp`
- `nes-logical-operators/tests/SpatioTemporalOperatorsTest.cpp`
- `nes-physical-operators/tests/SpatioTemporalPhysicalOperatorsTest.cpp`

#### Step 8.2: Add integration tests
**Location**: `nes-systests/tests/`

Create end-to-end tests:
- `SpatioTemporalQueryExecutionTest.cpp`
- `MeosIntegrationTest.cpp`

### 9. Documentation

#### Step 9.1: Update documentation
**Location**: `docs/`

Create documentation:
- `docs/technical/spatiotemporal-operations.md`
- `docs/development/meos-integration.md`

## Implementation Order

1. **Dependencies**: Set up MEOS library integration in build system
2. **Data Types**: Implement spatiotemporal data types
3. **Logical Operators**: Define logical operator interfaces
4. **Physical Operators**: Implement actual MEOS-based operations
5. **Code Generation**: Add compiler support
6. **SQL Parser**: Extend query language (optional)
7. **Testing**: Comprehensive test suite
8. **Documentation**: Update user and developer docs

## Key Considerations

### Performance
- MEOS operations can be computationally expensive
- Consider caching and memoization strategies
- Profile memory usage for large spatiotemporal datasets

### Memory Management
- MEOS objects need careful memory management
- Integrate with NebulaStream's memory management system
- Consider object pooling for frequent allocations

### Error Handling
- MEOS functions may fail or return invalid results
- Implement proper error propagation through the pipeline
- Add logging for debugging spatiotemporal operations

### Compatibility
- Ensure MEOS version compatibility
- Test with different spatial reference systems
- Validate temporal precision and time zones

This integration will provide NebulaStream with powerful spatiotemporal processing capabilities while maintaining the system's high-performance characteristics. 