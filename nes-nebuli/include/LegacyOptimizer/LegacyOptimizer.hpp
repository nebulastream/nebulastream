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

#include <LegacyOptimizer/QueryPlanning.hpp>

namespace NES
{

class LegacyOptimizer
{
    QueryPlanningContext& ctx; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime is managed by QueryPlanner

    explicit LegacyOptimizer(QueryPlanningContext& ctx) : ctx{ctx} { }

public:
    static LegacyOptimizer with(QueryPlanningContext& ctx) { return LegacyOptimizer{ctx}; }

    PlanStage::OptimizedLogicalPlan optimize(PlanStage::BoundLogicalPlan&& inputPlan) &&;
};

}
