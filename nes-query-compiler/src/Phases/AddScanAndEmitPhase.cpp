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

PipelinedQueryPlan AddScanAndEmitPhase::apply(const PipelinedQueryPlan& pipelineQueryPlan)
{
    for (const auto& pipeline : pipelineQueryPlan.pipelines)
    {
        // OperatorPipelines only
        if (auto* opPipeline = dynamic_cast<OperatorPipeline*>(pipeline.get()))
        {
            /// Scan
            PRECONDITION(opPipeline->hasOperators(), "A pipeline should have at least one root operator");
            auto newScan = std::make_unique<DefaultScanPhysicalOperator>(opPipeline->rootOperator,
                                                                         std::vector<Nautilus::Record::RecordFieldIdentifier>{});
            opPipeline->prependOperator(std::move(newScan));

            /// Emit
            for (auto* leaf : helper::getAllLeafNodes(*opPipeline->rootOperator))
            {
                if (auto* leafPhys = dynamic_cast<Operator*>(leaf); leafPhys)
                {
                    constexpr uint64_t bufferSize = 100; /// TODO change
                    auto memoryProvider = TupleBufferMemoryProvider::create(bufferSize, );
                    auto emitOperator = std::make_unique<DefaultEmitPhysicalOperator>(memoryProvider);
                    leafPhys->children.push_back(std::move(emitOperator));
                }
            }
        }
    }
    return pipelineQueryPlan;
}
}