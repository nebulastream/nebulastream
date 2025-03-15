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

#include <span>
#include <memory>
#include <utility>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnaryLogicalFunction.hpp>

namespace NES
{
UnaryLogicalFunction::UnaryLogicalFunction(std::unique_ptr<LogicalFunction> child)
    : LogicalFunction()
{
    children[0] = std::move(child);
}

UnaryLogicalFunction::UnaryLogicalFunction(const UnaryLogicalFunction& other) : LogicalFunction(other)
{
}

void UnaryLogicalFunction::setChild(std::unique_ptr<LogicalFunction> child)
{
    children[0] = std::move(child);
}

LogicalFunction& UnaryLogicalFunction::getChild() const
{
    return *children[0];
}

std::span<const std::unique_ptr<LogicalFunction>> UnaryLogicalFunction::getChildren() const
{
    return children;
}

}
