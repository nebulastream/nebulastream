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

#include <Execution/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Plans/DecomposedQueryPlan.hpp>
#include <Plans/OperatorPipeline.hpp>
#include <Plans/PipelineQueryPlan.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>

namespace NES::QueryCompilation::AddScanAndEmitPhase
{

std::shared_ptr<PipelineQueryPlan> apply(const std::shared_ptr<PhysicalQueryPlan>& physicalQueryPlan, const uint64_t bufferSize)
{
    std::unordered_map<OperatorPtr, std::shared_ptr<OperatorPipeline>> pipelineOperatorMap;
    auto pipelinePlan = std::make_shared<PipelineQueryPlan>(physicalQueryPlan->queryId);

    for (const auto& pipeline : pipelinePlan->pipelines)
    {
        if (pipeline->isOperatorPipeline())
        {
            auto queryPlan = pipeline->getPhysicalQueryPlan();
            auto pipelineRootOperators = queryPlan->rootOperators;
            PRECONDITION(!pipelineRootOperators.empty(), "A pipeline should have at least one root operator");

            /// insert buffer scan operator to the pipeline root if necessary
            const auto& rootOperator = pipelineRootOperators[0];
            if (!std::holds_alternative<std::shared_ptr<Scan>>(rootOperator->op)) // Do not add the root operator twice
            {
                auto schema = Schema::create(); // TODO this is a fake schema. Not sure from where to get it though
                INVARIANT(schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT, "Currently only row layout is supported");

                auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema, bufferSize);
                std::unique_ptr<MemoryProvider::TupleBufferMemoryProvider> memoryProvider
                    = std::make_unique<MemoryProvider::RowTupleBufferMemoryProvider>(layout);
                auto scanOperator = std::make_shared<Scan>(std::move(memoryProvider), schema->getFieldNames());
                pipeline->prependOperator(std::make_shared<PhysicalOperatorNode>(scanOperator));
            }

            /// insert emit buffer operator if necessary
            auto pipelineLeafOperators = rootOperator->getAllLeafNodes();
            for (const auto& leaf : pipelineLeafOperators)
            {
                if (!std::holds_alternative<std::shared_ptr<Emit>>(leaf->op)) // Do not add the emit operator twice
                {
                    auto schema = Schema::create(); // TODO this is a fake schema. Not sure from where to get it though
                    INVARIANT(schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT, "Currently only row layout is supported");

                    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema, bufferSize);
                    std::unique_ptr<MemoryProvider::TupleBufferMemoryProvider> memoryProvider
                        = std::make_unique<MemoryProvider::RowTupleBufferMemoryProvider>(layout);
                    auto emitOperator = std::make_shared<Emit>(std::move(memoryProvider));
                    leaf->children.push_back(std::make_shared<PhysicalOperatorNode>(emitOperator));
                }
            }
        }
    }
    for (const auto& pipeline : pipelinePlan->pipelines)
    {
        std::cout << pipeline->toString() << "\n";
    }
    return pipelinePlan;
}
}
