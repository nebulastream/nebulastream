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

#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

PlanIterator::PlanIterator(QueryPlanPtr queryPlan) { rootOperators = queryPlan->getRootOperators(); };

PlanIterator::PlanIterator(DecomposedQueryPlanPtr decomposedQueryPlan) {
    rootOperators = decomposedQueryPlan->getRootOperators();
}

PlanIterator::iterator PlanIterator::begin() { return iterator(rootOperators); }

PlanIterator::iterator PlanIterator::end() { return iterator(); }

std::vector<NodePtr> PlanIterator::snapshot() {
    std::vector<NodePtr> nodes;
    for (auto node : *this) {
        nodes.emplace_back(node);
    }
    return nodes;
}

PlanIterator::iterator::iterator(const std::vector<OperatorNodePtr>& rootOperators) {
    for (int64_t i = rootOperators.size() - 1; i >= 0; i--) {
        workStack.push(rootOperators[i]);
    }
}

PlanIterator::iterator::iterator() = default;

bool PlanIterator::iterator::operator!=(const iterator& other) const {
    if (workStack.empty() && other.workStack.empty()) {
        return false;
    };
    return true;
};

NodePtr PlanIterator::iterator::operator*() { return workStack.empty() ? nullptr : workStack.top(); }

PlanIterator::iterator& PlanIterator::iterator::operator++() {
    if (workStack.empty()) {
        NES_DEBUG("Iterator: we reached the end of this iterator and will not do anything.");
    } else {
        auto current = workStack.top();
        workStack.pop();
        auto children = current->getChildren();
        for (int64_t i = children.size() - 1; i >= 0; i--) {

            auto child = children[i];
            NES_ASSERT(!child->getParents().empty(), "A child node should have a parent");

            // check if current node is last parent of child.
            if (child->getParents().back() == current) {
                workStack.push(child);
            }
        }
    }
    return *this;
}

}// namespace NES