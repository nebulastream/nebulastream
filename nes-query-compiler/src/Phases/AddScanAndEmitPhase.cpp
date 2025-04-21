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
#include <utility>
#include <vector>
#include <Phases/AddScanAndEmitPhase.hpp>
#include <DefaultScanPhysicalOperator.hpp>
#include <DefaultEmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <PipelinedQueryPlan.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Pipeline.hpp>
#include <Plans/Operator.hpp>

namespace NES::QueryCompilation
{

namespace helper
{
[[nodiscard]] std::vector<Operator*> getAllLeafNodes(Operator& op)
{
    std::vector<Operator*> leaves;
    auto children = op.getChildren();
    if (children.empty())
    {
        leaves.push_back(&op);
    }
    else
    {
        for (const auto& child : children)
        {
            auto childLeaves = getAllLeafNodes(*child);
            leaves.insert(leaves.end(), childLeaves.begin(), childLeaves.end());
        }
    }
    return leaves;
}
}

std::unique_ptr<PipelinedQueryPlan> AddScanAndEmitPhase::apply(std::unique_ptr<PipelinedQueryPlan> pipelineQueryPlan)
{
    for (const auto& pipeline : pipelineQueryPlan->pipelines)
    {
        // OperatorPipelines only
        if (pipeline->isOperatorPipeline())
        {
            /// Scan
            std::vector<Nautilus::Record::RecordFieldIdentifier> emptyProjections {}; // TODO change
            constexpr uint64_t bufferSize = 100; /// TODO change
            constexpr uint64_t operatorHandlerIndex = 1; /// TODO change
            if (auto leafPhys = pipeline->getOperator<PhysicalOperatorWithSchema>(); leafPhys.has_value())
            {
                auto memoryProvider = TupleBufferMemoryProvider::create(bufferSize, leafPhys.value()->inputSchema);
                auto newScan = std::make_unique<DefaultScanPhysicalOperator>(std::move(memoryProvider), emptyProjections);
                ///pipeline->prependOperator(std::move(newScan));
            }

            /// Emit
            /*for (auto* leaf : helper::getAllLeafNodes(pipeline->pipelineOperator))
            {
                if (auto* leafPhys = dynamic_cast<PhysicalOperatorWithSchema*>(leaf); leafPhys)
                {
                    constexpr uint64_t bufferSize = 100; /// TODO change
                    constexpr uint64_t operatorHandlerIndex = 1; /// TODO change
                    auto memoryProvider = TupleBufferMemoryProvider::create(bufferSize, leafPhys->outputSchema);
                    auto emitOperator = std::make_unique<DefaultEmitPhysicalOperator>(operatorHandlerIndex, std::move(memoryProvider));
                    ///leafPhys->child = std::move(physicalOp);
                }
            }*/
        }
    }
    return pipelineQueryPlan;
}
}