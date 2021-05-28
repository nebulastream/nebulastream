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

#include <Topology/TopologyIterator.hpp>

namespace NES {

TopologyIterator::TopologyIterator(TopologyPtr topology) : topology(std::move(topology)){}

TopologyIterator::iterator TopologyIterator::begin() { return iterator(topology); }

TopologyIterator::iterator TopologyIterator::end() { return iterator(); }

std::vector<NodePtr> TopologyIterator::snapshot() {
    std::vector<NodePtr> nodes;
    for (auto node : *this) {
        nodes.emplace_back(node);
    }
    return nodes;
}

TopologyIterator::iterator::iterator(TopologyPtr current) {
    auto rootTopologyNode = current->getRoot();
    workStack.push(rootTopologyNode);
}

TopologyIterator::iterator::iterator() = default;

bool TopologyIterator::iterator::operator!=(const iterator& other) const {
    if (workStack.empty() && other.workStack.empty()) {
        return false;
    };
    return true;
};

NodePtr TopologyIterator::iterator::operator*() { return workStack.empty() ? nullptr : workStack.top(); }

TopologyIterator::iterator& TopologyIterator::iterator::operator++() {
    if (workStack.empty()) {
        NES_DEBUG("Iterator: we reached the end of this iterator and will not do anything.");
    } else {
        auto current = workStack.top();
        workStack.pop();
        auto children = current->getChildren();
        for (int64_t i = children.size() - 1; i >= 0; i--) {

            auto child = children[i];
            NES_ASSERT(child->getParents().size() != 0, "A child node should have a parent");

            // check if current node is last parent of child.
            if (child->getParents().back() == current) {
                workStack.push(child);
            }
        }
    }
    return *this;
}

} // namespace NES