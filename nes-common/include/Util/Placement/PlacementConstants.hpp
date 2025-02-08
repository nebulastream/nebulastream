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

#ifndef NES_COMMON_INCLUDE_UTIL_PLACEMENT_PLACEMENTCONSTANTS_HPP_
#define NES_COMMON_INCLUDE_UTIL_PLACEMENT_PLACEMENTCONSTANTS_HPP_

#include <string>

namespace NES::Optimizer {

const std::string PINNED_WORKER_ID = "PINNED_WORKER_ID";// Property indicating the location where the operator is pinned
const std::string LIST_OF_SOURCE_WORKER_ID =
    "LIST_OF_SOURCE_WORKER_ID";// Property indicating the location where the operator is pinned
const std::string PLACED_DECOMPOSED_PLAN_ID =
    "PLACED_DECOMPOSED_PLAN_ID";          // Property indicating the id of the decomposed plan that contains this operator
const std::string PROCESSED = "PROCESSED";// Property indicating if operator was processed for placement
const std::string CO_LOCATED_UPSTREAM_OPERATORS =
    "CO_LOCATED_UPSTREAM_OPERATORS";// Property indicating if operator was processed for placement
const std::string CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS =
    "CONNECTED_SYS_SUB_PLAN_DETAILS";// Property describing the details of the connected downstream system generated sub plans locations and query sub plan ids
const std::string UPSTREAM_LOGICAL_OPERATOR_ID =
    "UPSTREAM_LOGICAL_OPERATOR_ID";// Property containing the id of the next upstream non system operator of a system generated operator
const std::string DOWNSTREAM_LOGICAL_OPERATOR_ID =
    "DOWNSTREAM_LOGICAL_OPERATOR_ID";// Property containing the id of the next downstream non system operator of a system generated operator
}// namespace NES::Optimizer

#endif// NES_COMMON_INCLUDE_UTIL_PLACEMENT_PLACEMENTCONSTANTS_HPP_
