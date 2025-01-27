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
#include <vector>
#include <Nodes/Node.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

PlanIterator::PlanIterator(const QueryPlan& queryPlan)
{
    rootOperators = queryPlan.getRootOperators();
};

PlanIterator::PlanIterator(const DecomposedQueryPlan& decomposedQueryPlan)
{
    rootOperators = decomposedQueryPlan.getRootOperators();
}

PlanIterator::Iterator PlanIterator::begin()
{
    return Iterator(rootOperators);
}

PlanIterator::Iterator PlanIterator::end()
{
    return Iterator();
}

std::vector<std::shared_ptr<Node>> PlanIterator::snapshot()
{
    std::vector<std::shared_ptr<Node>> nodes;
    for (auto node : *this)
    {
        nodes.emplace_back(node);
    }
    return nodes;
}

PlanIterator::Iterator::Iterator(const std::vector<std::shared_ptr<Operator>>& rootOperators)
{
    for (int64_t i = rootOperators.size() - 1; i >= 0; i--)
    {
        workStack.push(rootOperators[i]);
    }
}

PlanIterator::Iterator::Iterator() = default;

bool PlanIterator::Iterator::operator!=(const Iterator& other) const
{
    if (workStack.empty() && other.workStack.empty())
    {
        return false;
    }
    return true;
};

std::shared_ptr<Node> PlanIterator::Iterator::operator*()
{
    return workStack.empty() ? nullptr : workStack.top();
}

PlanIterator::Iterator& PlanIterator::Iterator::operator++()
{
    if (workStack.empty())
    {
        NES_DEBUG("Iterator: we reached the end of this Iterator and will not do anything.");
    }
    else
    {
        auto current = workStack.top();
        workStack.pop();
        auto children = current->getChildren();
        for (int64_t i = children.size() - 1; i >= 0; i--)
        {
            auto child = children[i];
            INVARIANT(!child->getParents().empty(), "A child node must have a parent");

            /// check if current node is last parent of child.
            if (child->getParents().back() == current)
            {
                workStack.push(child);
            }
        }
    }
    return *this;
}

}
