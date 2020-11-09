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
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <utility>

namespace NES {

BreadthFirstNodeIterator::BreadthFirstNodeIterator(NodePtr start) : start(std::move(start)){};

BreadthFirstNodeIterator::iterator BreadthFirstNodeIterator::begin() { return iterator(start); }

BreadthFirstNodeIterator::iterator BreadthFirstNodeIterator::end() { return iterator(); }

BreadthFirstNodeIterator::iterator::iterator(NodePtr current) {
    workQueue.push(current);
}

BreadthFirstNodeIterator::iterator::iterator() = default;

bool BreadthFirstNodeIterator::iterator::operator!=(const iterator& other) const {
    // todo currently we only check if we reached the end of the iterator.
    if (workQueue.empty() && other.workQueue.empty()) {
        return false;
    };
    return true;
}

NodePtr BreadthFirstNodeIterator::iterator::operator*() { return workQueue.front(); }

BreadthFirstNodeIterator::iterator& BreadthFirstNodeIterator::iterator::operator++() {
    if (workQueue.empty()) {
        NES_DEBUG("BF Iterator: we reached the end of this iterator and will not do anything.");
    } else {
        auto current = workQueue.front();
        workQueue.pop();
        for (auto child : current->getChildren()) {
            workQueue.push(child);
        }
    }
    return *this;
}
}// namespace NES
