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

#include <cstdint>

namespace NES {

using SubpartitionId = uint64_t;
using PartitionId = uint64_t;
using OperatorId = uint64_t;
using OriginId = uint64_t;
using QueryId = uint64_t;
using PipelineId = uint64_t;
using SharedQueryId = uint64_t;
using DecomposedQueryPlanId = uint64_t;
using WorkerId = uint64_t;       // a unique identifier of the worker node or topology node
using ExecutionNodeId = uint64_t;// a unique identifier of the execution node holding all the placed sub query plans
using RequestId = uint64_t;
using DecomposedQueryPlanVersion = uint16_t;
static constexpr QueryId INVALID_QUERY_ID = 0;
static constexpr DecomposedQueryPlanId INVALID_DECOMPOSED_QUERY_PLAN_ID = 0;
static constexpr SharedQueryId INVALID_SHARED_QUERY_ID = 0;
static constexpr OperatorId INVALID_OPERATOR_ID = 0;
static constexpr OriginId INVALID_ORIGIN_ID = 0;
static constexpr WorkerId INVALID_WORKER_NODE_ID = 0;
static constexpr WorkerId INVALID_EXECUTION_NODE_ID = 0;
static constexpr RequestId INVALID_REQUEST_ID = 0;

}// namespace NES

#endif// NES_COMMON_INCLUDE_IDENTIFIERS_HPP_
