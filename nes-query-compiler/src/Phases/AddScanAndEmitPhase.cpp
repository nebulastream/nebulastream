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
#include <Plans/OperatorUtil.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <PipelinedQueryPlan.hpp>
#include <ScanPhysicalOperator.hpp>
#include <Phases/AddScanAndEmitPhase.hpp>

namespace NES::QueryCompilation
{

std::shared_ptr<PipelinedQueryPlan> AddScanAndEmitPhase::apply(std::shared_ptr<PipelinedQueryPlan> pipelineQueryPlan)
{
    /*
    for (const auto& pipeline : pipelineQueryPlan->pipelines)
    {
        if (NES::Util::instanceOf<OperatorPipeline>(pipeline))
        {
            auto opPipeline = Util::as<OperatorPipeline>(pipeline);
            PRECONDITION(opPipeline->hasOperators(), "A pipeline should have at least one root operator");

            /// insert buffer scan operator to the pipeline root if necessary
            const auto& rootOperator = opPipeline
            if (true) // TODO check if we need ta add a scan
            {
                auto newScan = std::make_shared<ScanPhysicalOperator>(
                    rootOperator->memoryProviders,
                    std::vector<Nautilus::Record::RecordFieldIdentifier>{});
                opPipeline->operators.insert(opPipeline->operators.begin(), newScan);
            }

            /// insert emit buffer operator if necessary
            auto pipelineLeafOperators = getAllLeafNodes(rootOperator);
            for (const auto& leaf : pipelineLeafOperators)
            {
                auto leafOperator = NES::Util::as<PhysicalOperator>(leaf);
                if (true) // TODO check if we need to add an emit
                {
                    auto emitOperator = EmitPhysicalOperator(1 TODO, std::move(leafOperator->memoryProviders[0]));
                    leafOperator->children.push_back(emitOperator); /// TODO its weird that this does not take a shared ptr
                }
            }
        }
    }*/
    return pipelineQueryPlan;
}
}