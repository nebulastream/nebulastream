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

#ifndef NES_INCLUDE_OPTIMIZER_PHASES_SETMEMORYLAYOUTPHASE_HPP
#define NES_INCLUDE_OPTIMIZER_PHASES_SETMEMORYLAYOUTPHASE_HPP

#include <memory>
#include <vector>
#include <Plans/Query/QueryPlan.hpp>
#include <API/Schema.hpp>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;
}// namespace NES

namespace NES::Optimizer {

class SetMemoryLayoutPhase;
using SetMemoryLayoutPhasePtr = std::shared_ptr<SetMemoryLayoutPhase>;


class SetMemoryLayoutPhase {
  public:
    /**
     * @brief Method for creating a choose mem layout phase
     * @param layoutType
     * @return QueryChooseMemLayoutPhasePtr
     */
    static SetMemoryLayoutPhasePtr create(Schema::MemoryLayoutType layoutType);

    /**
     * @brief Sets either all sources to col or row layout
     * @param queryPlan : the input query plan
     */
    void execute(const QueryPlanPtr& queryPlan);

  private:
    /**
     * @brief Constructor for QueryChooseMemLayoutPhase
     * @param layoutType
     */
    SetMemoryLayoutPhase(Schema::MemoryLayoutType layoutType);

  private:
    Schema::MemoryLayoutType layoutType;
};
}

#endif//NES_INCLUDE_OPTIMIZER_PHASES_SETMEMORYLAYOUTPHASE_HPP
