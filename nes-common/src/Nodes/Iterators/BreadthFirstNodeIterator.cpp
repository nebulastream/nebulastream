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
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

BreadthFirstNodeIterator::BreadthFirstNodeIterator(std::shared_ptr<Node> start) : start(std::move(start)) { };

BreadthFirstNodeIterator::Iterator BreadthFirstNodeIterator::begin() const
{
    return Iterator(start);
}

BreadthFirstNodeIterator::Iterator BreadthFirstNodeIterator::end()
{
    return Iterator();
}

BreadthFirstNodeIterator::Iterator::Iterator(const std::shared_ptr<Node>& current)
{
    workQueue.push(current);
}

BreadthFirstNodeIterator::Iterator::Iterator() = default;

bool BreadthFirstNodeIterator::Iterator::operator!=(const Iterator& other) const
{
    /// todo currently we only check if we reached the end of the iterator.
    if (workQueue.empty() && other.workQueue.empty())
    {
        return false;
    }
    return true;
}

std::shared_ptr<Node> BreadthFirstNodeIterator::Iterator::operator*()
{
    return workQueue.front();
}

BreadthFirstNodeIterator::Iterator& BreadthFirstNodeIterator::Iterator::operator++()
{
    if (workQueue.empty())
    {
        NES_DEBUG("BF Iterator: we reached the end of this iterator and will not do anything.");
    }
    else
    {
        auto current = workQueue.front();
        workQueue.pop();
        for (const auto& child : current->getChildren())
        {
            workQueue.push(child);
        }
    }
    return *this;
}
}
