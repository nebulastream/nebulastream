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

#include <Util/Logger.hpp>
#include <Optimizer/Phases/QueryChooseMemLayoutPhase.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>

namespace NES::Optimizer {
void QueryChooseMemLayoutPhase::execute(QueryPlanPtr queryPlan) {
    try {
        // Getting all sources and sinks from this query plan
        auto sources = queryPlan->getSourceOperators();
        auto sinks = queryPlan->getSinkOperators();

        if (!sources.empty()) {
            NES_WARNING("QueryChooseMemLayoutPhase: Sources are empty!");
        }

        if (!sinks.empty()) {
            NES_WARNING("QueryChooseMemLayoutPhase: Sinks are empty!");
        }

        for (auto& source : sources) {
            // TODO is this enough? Should I maybe create a setter in schema?
            source->getSourceDescriptor()->getSchema()->layoutType = layoutType;
        }

        for (auto& sink : sinks) {
            // TODO what should I do here?
            ((void)sink);
        }


    } catch (Exception& e) {
        NES_THROW_RUNTIME_ERROR("QueryChooseMemLayoutPhase: Exception occurred during choose memory layout phase " << e.what());
    }
}

QueryChooseMemLayoutPhase::QueryChooseMemLayoutPhase(Schema::MemoryLayoutType layoutType) : layoutType(layoutType) {}
QueryChooseMemLayoutPhasePtr QueryChooseMemLayoutPhase::create(Schema::MemoryLayoutType layoutType) {
    return std::make_shared<QueryChooseMemLayoutPhase>(QueryChooseMemLayoutPhase(layoutType));
}

}