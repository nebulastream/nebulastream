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
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlanIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::QueryCompilation
{
PipelineQueryPlanIterator::PipelineQueryPlanIterator(PipelineQueryPlanPtr queryPlan) : queryPlan(std::move(queryPlan)) {};

PipelineQueryPlanIterator::iterator PipelineQueryPlanIterator::begin()
{
    return iterator(queryPlan);
}

PipelineQueryPlanIterator::iterator PipelineQueryPlanIterator::end()
{
    return iterator();
}

std::vector<OperatorPipelinePtr> PipelineQueryPlanIterator::snapshot()
{
    std::vector<OperatorPipelinePtr> nodes;
    for (auto node : *this)
    {
        nodes.emplace_back(node);
    }
    return nodes;
}

PipelineQueryPlanIterator::iterator::iterator(const PipelineQueryPlanPtr& current)
{
    auto rootOperators = current->getSourcePipelines();
    for (int64_t i = rootOperators.size() - 1; i >= 0; i--)
    {
        workStack.push(rootOperators[i]);
    }
}

PipelineQueryPlanIterator::iterator::iterator() = default;

bool PipelineQueryPlanIterator::iterator::operator!=(const iterator& other) const
{
    if (workStack.empty() && other.workStack.empty())
    {
        return false;
    };
    return true;
};

OperatorPipelinePtr PipelineQueryPlanIterator::iterator::operator*()
{
    return workStack.empty() ? nullptr : workStack.top();
}

PipelineQueryPlanIterator::iterator& PipelineQueryPlanIterator::iterator::operator++()
{
    if (workStack.empty())
    {
        NES_DEBUG("Iterator: we reached the end of this iterator and will not do anything.");
    }
    else
    {
        auto current = workStack.top();
        workStack.pop();
        auto children = current->getSuccessors();
        for (int64_t i = children.size() - 1; i >= 0; i--)
        {
            auto child = children[i];
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
