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

#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Util/Logger.hpp>

namespace NES::Optimizer {
QueryPlanPtr MemoryLayoutSelectionPhase::execute(const QueryPlanPtr& queryPlan) {

    Schema::MemoryLayoutType layoutType;
    switch (policy) {
        case FORCE_ROW_LAYOUT: {
            NES_DEBUG("Select Row Layout");
            layoutType = Schema::ROW_LAYOUT;
            break;
        }
        case FORCE_COLUMN_LAYOUT: {
            NES_DEBUG("Select Column Layout");
            layoutType = Schema::COLUMNAR_LAYOUT;
            break;
        }
    }

    // iterate over all operators and set the output schema
    auto iterator = QueryPlanIterator(queryPlan);
    for (auto node : iterator) {
        if (auto op = node->as_if<SourceLogicalOperatorNode>()) {
            op->getSourceDescriptor()->getSchema()->setLayoutType(layoutType);
        }
        if (auto op = node->as_if<LogicalUnaryOperatorNode>()) {
            op->getInputSchema()->setLayoutType(layoutType);
            op->getOutputSchema()->setLayoutType(layoutType);
        }
        if (auto op = node->as_if<LogicalBinaryOperatorNode>()) {
            op->getLeftInputSchema()->setLayoutType(layoutType);
            op->getRightInputSchema()->setLayoutType(layoutType);
            op->getOutputSchema()->setLayoutType(layoutType);
        }
    }
    return queryPlan;
}

MemoryLayoutSelectionPhase::MemoryLayoutSelectionPhase(MemoryLayoutPolicy policy) : policy(policy) {}

MemoryLayoutSelectionPhasePtr MemoryLayoutSelectionPhase::create(MemoryLayoutPolicy policy) {
    return std::make_shared<MemoryLayoutSelectionPhase>(MemoryLayoutSelectionPhase(policy));
}

}// namespace NES::Optimizer