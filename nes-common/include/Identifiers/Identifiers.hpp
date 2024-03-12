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

#ifndef NES_COMMON_INCLUDE_IDENTIFIERS_HPP_
#define NES_COMMON_INCLUDE_IDENTIFIERS_HPP_

#include <Identifiers/NESStrongType.hpp>
#include <cstdint>

#define UNSURE_CONVERSION_TODO_4761(from, to) (to(from.getRawValue()))

namespace NES {

//                      data type, struct name, invalid value, initial value
using SubpartitionId = NESStrongType<uint64_t, struct SubpartitionId_, 0, 1>;
using PartitionId = NESStrongType<uint64_t, struct PartitionId_, 0, 1>;
using OperatorId = NESStrongType<uint64_t, struct OperatorId_, 0, 1>;
using OriginId = NESStrongType<uint64_t, struct OriginId_, 0, 1>;
using PipelineId = NESStrongType<uint64_t, struct PipelineId_, 0, 1>;
using QueryId = NESStrongType<uint64_t, struct QueryId_, 0, 1>;
using SharedQueryId = NESStrongType<uint64_t, struct SharedQueryId_, 0, 1>;
using DecomposedQueryPlanId = NESStrongType<uint64_t, struct DecomposedQueryPlanId_, 0, 1>;
using WorkerId = NESStrongType<uint64_t, struct WorkerId_, 0, 1>;// a unique identifier of the worker node or topology node
using RequestId = NESStrongType<uint64_t, struct RequestId_, 0, 1>;

using StatisticId = uint64_t;// Unique identifier across the system so that we can track statistic over the item
using DecomposedQueryPlanVersion = uint16_t;
using WatermarkTs = uint64_t;
using SequenceNumber = uint64_t;
using ChunkNumber = uint64_t;

static constexpr QueryId INVALID_QUERY_ID = INVALID<QueryId>;
static constexpr QueryId INITIAL_QUERY_ID = INITIAL<QueryId>;

static constexpr DecomposedQueryPlanId INVALID_DECOMPOSED_QUERY_PLAN_ID = INVALID<DecomposedQueryPlanId>;
static constexpr DecomposedQueryPlanId INITIAL_DECOMPOSED_QUERY_PLAN_ID = INITIAL<DecomposedQueryPlanId>;

static constexpr SharedQueryId INVALID_SHARED_QUERY_ID = INVALID<SharedQueryId>;
static constexpr SharedQueryId INITIAL_SHARED_QUERY_ID = INITIAL<SharedQueryId>;

static constexpr OperatorId INVALID_OPERATOR_ID = INVALID<OperatorId>;
static constexpr OperatorId INITIAL_OPERATOR_ID = INITIAL<OperatorId>;

static constexpr OriginId INVALID_ORIGIN_ID = INVALID<OriginId>;
static constexpr OriginId INITIAL_ORIGIN_ID = INITIAL<OriginId>;

static constexpr PipelineId INVALID_PIPELINE_ID = INVALID<PipelineId>;
static constexpr PipelineId INITIAL_PIPELINE_ID = INITIAL<PipelineId>;
static constexpr StatisticId INVALID_STATISTIC_ID = 0;
static constexpr WorkerId INVALID_WORKER_NODE_ID = INVALID<WorkerId>;
static constexpr WorkerId INITIAL_WORKER_NODE_ID = INITIAL<WorkerId>;

static constexpr DecomposedQueryPlanVersion INVALID_DECOMPOSED_QUERY_PLAN_VERSION = 0;
static constexpr RequestId INVALID_REQUEST_ID = INVALID<RequestId>;
static constexpr ChunkNumber INVALID_CHUNK_NUMBER = 0;
static constexpr SequenceNumber INVALID_SEQ_NUMBER = 0;

}// namespace NES

#endif// NES_COMMON_INCLUDE_IDENTIFIERS_HPP_
