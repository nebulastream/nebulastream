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
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

DepthFirstNodeIterator::DepthFirstNodeIterator(std::shared_ptr<Node> start) : start(std::move(start)) {};

DepthFirstNodeIterator::Iterator DepthFirstNodeIterator::begin() const
{
    return Iterator(start);
}

DepthFirstNodeIterator::Iterator DepthFirstNodeIterator::end()
{
    return Iterator();
}

DepthFirstNodeIterator::Iterator::Iterator(const std::shared_ptr<Node>& current)
{
    workStack.push(current);
}

DepthFirstNodeIterator::Iterator::Iterator() = default;

bool DepthFirstNodeIterator::Iterator::operator!=(const Iterator& other) const
{
    /// todo currently we only check if we reached the end of the iterator.
    if (workStack.empty() && other.workStack.empty())
    {
        return false;
    }
    return true;
}

std::shared_ptr<Node> DepthFirstNodeIterator::Iterator::operator*()
{
    return workStack.top();
}

DepthFirstNodeIterator::Iterator& DepthFirstNodeIterator::Iterator::operator++()
{
    if (workStack.empty())
    {
        NES_DEBUG("DF Iterator: we reached the end of this iterator and will not do anything.");
    }
    else
    {
        auto current = workStack.top();
        workStack.pop();
        for (const auto& child : current->getChildren())
        {
            workStack.push(child);
        }
    }
    return *this;
}
}
