/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_OPTIMIZER_PHASES_MEMORYLAYOUTSELECTIONPHASE_HPP_
#define NES_INCLUDE_OPTIMIZER_PHASES_MEMORYLAYOUTSELECTIONPHASE_HPP_

#include <API/Schema.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <memory>
#include <vector>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;
}// namespace NES

namespace NES::Optimizer {

class MemoryLayoutSelectionPhase;
using MemoryLayoutSelectionPhasePtr = std::shared_ptr<MemoryLayoutSelectionPhase>;

/**
 * @brief This optimization phase selects a memory layout for a particular query.
 * To this end, it traverses all operators in a query plan and sets the selected memory layout information to the schema.
 * Currently the phase supports two MemoryLayoutPolicies:
 * 1. FORCE_ROW_LAYOUT:
 *      This policy enforces a row layout for all operators within an query.
 * 2. FORCE_COLUMN_LAYOUT (experimental):
 *      This policy enforces a column layout for all operators within an query.
 *
 * This phase can't guarantee that a selected layout is really executable, as currently not all data sources support the columnar layout.
 * This would result in failed queries at the worker side.
 * Therefore the column layout is currently only experimental.
 */
class MemoryLayoutSelectionPhase {
  public:
    /**
     * @brief Represents a particular MemoryLayoutPolicy to select different strategies.
     */
    enum MemoryLayoutPolicy : uint8_t {
        // Enforces a row layout for all operators within a query.
        FORCE_ROW_LAYOUT,
        // Enforces a columnar layout for all operators within a query.
        FORCE_COLUMN_LAYOUT
    };

    /**
     * @brief Method for creating a new MemoryLayoutSelectionPhase requires to pass the memory layout policy
     * @param policy to select the memory layout for a query
     * @return MemoryLayoutSelectionPhasePtr
     */
    static MemoryLayoutSelectionPhasePtr create(MemoryLayoutPolicy policy);

    /**
     * @brief Selects the memory layout for all operators within an query.
     * @param queryPlan : the input query plan
     */
    QueryPlanPtr execute(const QueryPlanPtr& queryPlan);

  private:
    /**
     * @brief Constructor for MemoryLayoutSelectionPhase
     * @param policy to select the memory layout for a query
     */
    MemoryLayoutSelectionPhase(MemoryLayoutPolicy policy);

  private:
    MemoryLayoutPolicy policy;
};

static const std::map<std::string, MemoryLayoutSelectionPhase::MemoryLayoutPolicy> stringToMemoryLayoutPolicy{
    {"FORCE_ROW_LAYOUT", MemoryLayoutSelectionPhase::MemoryLayoutPolicy::FORCE_ROW_LAYOUT},
    {"FORCE_COLUMN_LAYOUT", MemoryLayoutSelectionPhase::MemoryLayoutPolicy::FORCE_COLUMN_LAYOUT}};

}// namespace NES::Optimizer

#endif  // NES_INCLUDE_OPTIMIZER_PHASES_MEMORYLAYOUTSELECTIONPHASE_HPP_
