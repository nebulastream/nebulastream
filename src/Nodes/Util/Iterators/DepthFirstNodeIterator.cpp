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

#include <Nodes/Node.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <utility>

namespace NES {

DepthFirstNodeIterator::DepthFirstNodeIterator(NodePtr start) : start(std::move(start)){};

DepthFirstNodeIterator::iterator DepthFirstNodeIterator::begin() { return iterator(start); }

DepthFirstNodeIterator::iterator DepthFirstNodeIterator::end() { return iterator(); }

DepthFirstNodeIterator::iterator::iterator(NodePtr current) { workStack.push(current); }

DepthFirstNodeIterator::iterator::iterator() = default;

bool DepthFirstNodeIterator::iterator::operator!=(const iterator& other) const {
    // todo currently we only check if we reached the end of the iterator.
    if (workStack.empty() && other.workStack.empty()) {
        return false;
    };
    return true;
}

NodePtr DepthFirstNodeIterator::iterator::operator*() { return workStack.top(); }

DepthFirstNodeIterator::iterator& DepthFirstNodeIterator::iterator::operator++() {
    if (workStack.empty()) {
        NES_DEBUG("DF Iterator: we reached the end of this iterator and will not do anything.");
    } else {
        auto current = workStack.top();
        workStack.pop();
        for (auto child : current->getChildren()) {
            workStack.push(child);
        }
    }
    return *this;
}
}// namespace NES
