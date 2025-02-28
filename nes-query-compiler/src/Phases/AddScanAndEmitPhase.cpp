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

#include <memory>
#include <Phases/AddScanAndEmitPhase.hpp>
#include <Plans/OperatorUtil.hpp>
#include <DefaultEmitPhysicalOperator.hpp>
#include <DefaultScanPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <PipelinedQueryPlan.hpp>

namespace NES::QueryCompilation
{

PipelinedQueryPlan AddScanAndEmitPhase::apply(const PipelinedQueryPlan& pipelineQueryPlan)
{
    // Iterate over every pipeline in the query plan.
    for (auto& pipeline : pipelineQueryPlan.pipelines)
    {
        // Process only OperatorPipelines.
        if (auto* opPipeline = dynamic_cast<OperatorPipeline*>(pipeline.get()))
        {
            PRECONDITION(opPipeline->hasOperators(), "A pipeline should have at least one root operator");

            auto& rootOperator = opPipeline->operators.front();
            if (needsScan(*rootOperator))
            {
                auto newScan = std::make_unique<DefaultScanPhysicalOperator>(rootOperator->memoryProviders,
                    std::vector<Nautilus::Record::RecordFieldIdentifier>{});
                opPipeline->operators.insert(opPipeline->operators.begin(), std::move(newScan));
            }

            auto leafOperators = getAllLeafNodes(*rootOperator);
            for (auto* leaf : leafOperators)
            {
                if (auto* leafPhys = dynamic_cast<PhysicalOperator*>(leaf))
                {
                    if (needsEmit(*leafPhys))
                    {
                        auto emitOperator = std::make_unique<DefaultEmitPhysicalOperator>(std::move(leafPhys->memoryProviders[0]));
                        leafPhys->children.push_back(std::move(emitOperator));
                    }
                }
            }
        }
    }
    return pipelineQueryPlan;
}
}