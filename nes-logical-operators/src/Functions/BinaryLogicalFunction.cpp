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
#include <Functions/BinaryLogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

constexpr size_t LEFT_CHILD_INDEX = 0;
constexpr size_t RIGHT_CHILD_INDEX = 1;

BinaryLogicalFunction::BinaryLogicalFunction(std::shared_ptr<DataType> stamp, std::shared_ptr<LogicalFunction> left, std::shared_ptr<LogicalFunction> right)
    : LogicalFunction(std::move(stamp))
{
    children[LEFT_CHILD_INDEX] = left;
    children[RIGHT_CHILD_INDEX] = right;
}

BinaryLogicalFunction::BinaryLogicalFunction(const BinaryLogicalFunction& other) : LogicalFunction(other)
{
    children[LEFT_CHILD_INDEX] = other.getLeftChild()->clone();
    children[RIGHT_CHILD_INDEX] = other.getRightChild()->clone();
}

void BinaryLogicalFunction::setLeftChild(std::shared_ptr<LogicalFunction> left)
{
    children[LEFT_CHILD_INDEX] = left;
}

void BinaryLogicalFunction::setRightChild(std::shared_ptr<LogicalFunction> right)
{
    children[RIGHT_CHILD_INDEX] = right;
}

std::shared_ptr<LogicalFunction> BinaryLogicalFunction::getLeftChild() const
{
    return children[LEFT_CHILD_INDEX];
}

std::shared_ptr<LogicalFunction> BinaryLogicalFunction::getRightChild() const
{
    return children[RIGHT_CHILD_INDEX];
}

std::span<const std::shared_ptr<LogicalFunction>> BinaryLogicalFunction::getChildren() const
{
    return children;
}

}
