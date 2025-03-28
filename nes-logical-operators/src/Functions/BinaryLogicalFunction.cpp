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
#include <string>
#include <utility>
#include <Functions/BinaryLogicalFunction.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
BinaryLogicalFunction::BinaryLogicalFunction(std::shared_ptr<DataType> stamp, std::string name)
    : LogicalFunction(std::move(stamp), std::move(name))
{
}

BinaryLogicalFunction::BinaryLogicalFunction(BinaryLogicalFunction* other) : LogicalFunction(other)
{
    addChildWithEqual(getLeft()->deepCopy());
    addChildWithEqual(getRight()->deepCopy());
}

void BinaryLogicalFunction::setChildren(std::shared_ptr<LogicalFunction> const& left, std::shared_ptr<LogicalFunction> const& right)
{
    addChildWithEqual(left);
    addChildWithEqual(right);
}

std::shared_ptr<LogicalFunction> BinaryLogicalFunction::getLeft() const
{
    INVARIANT(children.size() == 2, "A binary function always should have two children, but it had: {}", std::to_string(children.size()));
    return Util::as<LogicalFunction>(children[0]);
}

std::shared_ptr<LogicalFunction> BinaryLogicalFunction::getRight() const
{
    INVARIANT(children.size() == 2, "A binary function always should have two children, but it had: {}", std::to_string(children.size()));
    return Util::as<LogicalFunction>(children[1]);
}

}
