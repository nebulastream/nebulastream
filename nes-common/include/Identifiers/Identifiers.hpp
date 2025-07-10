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

#include <cstddef>
#include <cstdint>
#include <Identifiers/NESStrongType.hpp>

namespace NES
{
/// TODO #601 Refactor Identifiers.hpp

/// Global Identifiers: These Identifiers are unique to the SingleNodeWorkers entire lifetime.
/// There can never exist two objects with the same Identifier, regardless if the previous object has been destroyed
using OperatorId = NESStrongType<uint64_t, struct OperatorId_, 0, 1>;
using OriginId = NESStrongType<uint64_t, struct OriginId_, 0, 1>;
using QueryId = NESStrongType<uint64_t, struct QueryId_, 0, 1>;
using WorkerThreadId = NESStrongType<uint32_t, struct WorkerThreadId_, UINT32_MAX, 0>;
using PhysicalSourceId = NESStrongType<uint64_t, struct PhysicalSourceId_, 0, 1>;

/// Local Identifiers: These Identifiers are unique in a local scope. E.g. the PipelineId is unique in regard to a single query plan.
using PipelineId = NESStrongType<uint64_t, struct PipelineId_, 0, 1>;
using SequenceNumber = NESStrongType<uint64_t, struct SequenceNumber_, 0, 1>;
using ChunkNumber = NESStrongType<uint64_t, struct ChunkNumber_, SequenceNumber::INVALID, SequenceNumber::INITIAL>;


static constexpr QueryId INVALID_QUERY_ID = INVALID<QueryId>;
static constexpr QueryId INITIAL_QUERY_ID = INITIAL<QueryId>;

static constexpr OperatorId INVALID_OPERATOR_ID = INVALID<OperatorId>;
static constexpr OperatorId INITIAL_OPERATOR_ID = INITIAL<OperatorId>;

static constexpr OriginId INVALID_ORIGIN_ID = INVALID<OriginId>;
static constexpr OriginId INITIAL_ORIGIN_ID = INITIAL<OriginId>;

static constexpr PhysicalSourceId INVALID_PHYSICAL_SOURCE_ID = INVALID<PhysicalSourceId>;
static constexpr PhysicalSourceId INITIAL_PHYSICAL_SOURCE_ID = INITIAL<PhysicalSourceId>;

static constexpr PipelineId INVALID_PIPELINE_ID = INVALID<PipelineId>;
static constexpr PipelineId INITIAL_PIPELINE_ID = INITIAL<PipelineId>;

static constexpr ChunkNumber INVALID_CHUNK_NUMBER = INVALID<ChunkNumber>;
static constexpr ChunkNumber INITIAL_CHUNK_NUMBER = INITIAL<ChunkNumber>;

static constexpr SequenceNumber INVALID_SEQ_NUMBER = INVALID<SequenceNumber>;
static constexpr SequenceNumber INITIAL_SEQ_NUMBER = INITIAL<SequenceNumber>;

/// Special overloads for commonly occurring patterns
/// overload modulo operator for WorkerThreadId as it is commonly use to index into buckets
inline size_t operator%(const WorkerThreadId id, const size_t containerSize)
{
    return id.getRawValue() % containerSize;
}

/// Legacy
using WorkerId = NESStrongType<uint64_t, struct WorkerId_, 0, 1>; /// a unique identifier of the worker node or topology node
static constexpr WorkerId INVALID_WORKER_NODE_ID = INVALID<WorkerId>;
static constexpr WorkerId INITIAL_WORKER_NODE_ID = INITIAL<WorkerId>;
}
