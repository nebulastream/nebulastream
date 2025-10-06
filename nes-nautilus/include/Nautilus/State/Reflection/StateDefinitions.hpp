/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once
#include <rfl.hpp>
#include <rfl/capnproto.hpp>
#include <vector>
#include <string>
#include <cstdint>
#include <Nautilus/DataStructures/SerializablePagedVector.hpp>
#include <Nautilus/DataStructures/OffsetBasedHashMap.hpp>

namespace NES::State {

using OffsetEntry = NES::DataStructures::OffsetEntry;

struct OperatorMetadata {
    uint64_t operatorId;
    uint32_t operatorType;
    uint64_t processedRecords = 0;
    uint64_t lastWatermark = 0;
    uint32_t version = 1;
    uint64_t droppedRecords = 0;
};

// Operation parameter types
struct SumParams {
    std::string fieldName;
    bool ignoreNulls = true;
};

struct CountParams {
    bool distinct = false;
};

struct AvgParams {
    std::string fieldName;
    bool ignoreNulls = true;
};

struct MinParams {
    std::string fieldName;
    bool ignoreNulls = true;
};

struct MaxParams {
    std::string fieldName;
    bool ignoreNulls = true;
};

// Tagged union for operation parameters
using OperationParams = rfl::TaggedUnion<
    "type",
    rfl::Field<"sum", SumParams>,
    rfl::Field<"count", CountParams>,
    rfl::Field<"avg", AvgParams>,
    rfl::Field<"min", MinParams>,
    rfl::Field<"max", MaxParams>
>;

struct Operation {
    uint32_t id;
    std::string name;
    OperationParams parameters;
};

struct BaseState {
    OperatorMetadata metadata;
    std::vector<Operation> operations;
};

struct AggregationState {
    // Base state fields (flattened to avoid inheritance issues with reflection)
    OperatorMetadata metadata;
    std::vector<Operation> operations;
    
    // Aggregation-specific fields
    struct Config {
        uint64_t keySize;
        uint64_t valueSize;
        uint64_t numberOfBuckets;
        uint64_t pageSize;
    } config;
    
    struct HashMap {
        std::vector<uint8_t> memory;      // All data in one block
        std::vector<uint32_t> chains;     // Head offsets for each chain
        uint64_t entrySize;
        uint64_t tupleCount = 0;
        
        // Metadata for reconstruction
        uint64_t keySize;
        uint64_t valueSize;
        uint64_t bucketCount;
        
        // Variable-sized data support (kept separate from main memory)
        std::vector<uint8_t> varSizedMemory;
        uint64_t varSizedOffset{0};
    };
    
    std::vector<HashMap> hashMaps;
    
    struct Window {
        uint64_t start;
        uint64_t end;
        uint32_t state;
        // Index into AggregationState.hashMaps where this window's first hash map is stored
        uint64_t firstHashMapIndex{0};
        // Number of hash maps (across slices and worker threads) captured for this window
        uint64_t hashMapCount{0};
    };
    
    std::vector<Window> windows;
};

// Placeholder for other states (to be implemented)
struct JoinState {
    // Base state fields (flattened)
    OperatorMetadata metadata;
    std::vector<Operation> operations;

    // Join-specific configuration
    struct Config {
        uint64_t keySize;            // key size for hash map (HJ) or composite key size if needed
        uint64_t valueSize;          // payload aggregation size per entry (HJ) or tuple size for vectors
        uint64_t numberOfBuckets;    // HJ
        uint64_t pageSize;           // page size for paged vectors
    } config;

    // Hash map state (used by Hash Join build side)
    struct HashMap {
        std::vector<uint8_t> memory;
        std::vector<uint32_t> chains;
        uint64_t entrySize;
        uint64_t tupleCount = 0;

        // Reconstruction metadata
        uint64_t keySize;
        uint64_t valueSize;
        uint64_t bucketCount;

        // Variable-sized data (optional)
        std::vector<uint8_t> varSizedMemory;
        uint64_t varSizedOffset{0};
    };

    // Paged vectors (used by Nested Loop Join sides and Hash Join values)
    // We reuse the serializable paged vector state from Nautilus data structures
    using VectorState = NES::DataStructures::PagedVectorState;

    // Flat storage for all structures across windows; windows provide index ranges
    std::vector<HashMap> hashMaps;
    std::vector<VectorState> leftVectors;
    std::vector<VectorState> rightVectors;

    struct Window {
        uint64_t start;
        uint64_t end;
        uint32_t state; // impl-defined window state flags

        // Ranges for per-window state
        uint64_t firstHashMapIndex{0};
        uint64_t hashMapCount{0};
        uint64_t firstLeftVectorIndex{0};
        uint64_t leftVectorCount{0};
        uint64_t firstRightVectorIndex{0};
        uint64_t rightVectorCount{0};
    };

    std::vector<Window> windows;
};

struct FilterState {
    // Base state fields (flattened)  
    OperatorMetadata metadata;
    std::vector<Operation> operations;
    // Will be defined in Phase 5
};

// Tagged union for polymorphic state with type discriminator
using OperatorState = rfl::TaggedUnion<
    "type",
    rfl::Field<"aggregation", AggregationState>,
    rfl::Field<"join", JoinState>,
    rfl::Field<"filter", FilterState>
>;

}
