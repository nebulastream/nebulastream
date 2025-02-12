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
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlanIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::QueryCompilation
{
PipelineQueryPlanIterator::PipelineQueryPlanIterator(std::shared_ptr<PipelineQueryPlan> queryPlan) : queryPlan(std::move(queryPlan)) {};

PipelineQueryPlanIterator::Iterator PipelineQueryPlanIterator::begin()
{
    return Iterator(queryPlan);
}

PipelineQueryPlanIterator::Iterator PipelineQueryPlanIterator::end()
{
    return Iterator();
}

std::vector<std::shared_ptr<OperatorPipeline>> PipelineQueryPlanIterator::snapshot()
{
    std::vector<std::shared_ptr<OperatorPipeline>> nodes;
    for (auto node : *this)
    {
        nodes.emplace_back(node);
    }
    return nodes;
}

PipelineQueryPlanIterator::Iterator::Iterator(const std::shared_ptr<PipelineQueryPlan>& current)
{
    const auto rootOperators = current->getSourcePipelines();
    for (int64_t i = rootOperators.size() - 1; i >= 0; i--)
    {
        workStack.push(rootOperators[i]);
    }
}

PipelineQueryPlanIterator::Iterator::Iterator() = default;

bool PipelineQueryPlanIterator::Iterator::operator!=(const Iterator& other) const
{
    if (workStack.empty() && other.workStack.empty())
    {
        return false;
    };
    return true;
};

std::shared_ptr<OperatorPipeline> PipelineQueryPlanIterator::Iterator::operator*()
{
    return workStack.empty() ? nullptr : workStack.top();
}

PipelineQueryPlanIterator::Iterator& PipelineQueryPlanIterator::Iterator::operator++()
{
    if (workStack.empty())
    {
        NES_DEBUG("Iterator: we reached the end of this iterator and will not do anything.");
    }
    else
    {
        const auto current = workStack.top();
        workStack.pop();
        const auto children = current->getSuccessors();
        for (int64_t i = children.size() - 1; i >= 0; i--)
        {
            const auto& child = children[i];
            INVARIANT(!child->getPredecessors().empty(), "A child node should have a parent");

            /// check if current node is last parent of child.
            if (child->getSuccessors().back() == current)
            {
                workStack.push(child);
            }
        }
    }
    return *this;
}
}
