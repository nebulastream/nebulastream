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
#include <sstream>
#include <utility>
#include <Functions/ArithmeticalFunctions/AddBinaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

AddBinaryLogicalFunction::AddBinaryLogicalFunction(std::shared_ptr<DataType> stamp)
    : LogicalFunctionArithmeticalBinary(std::move(stamp), "Add") {};

AddBinaryLogicalFunction::AddBinaryLogicalFunction(AddBinaryLogicalFunction* other) : LogicalFunctionArithmeticalBinary(other)
{
}

std::shared_ptr<LogicalFunction>
AddBinaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto addNode = std::make_shared<AddBinaryLogicalFunction>(left->getStamp());
    addNode->setChildren(left, right);
    return addNode;
}

bool AddBinaryLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<AddBinaryLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<AddBinaryLogicalFunction>(rhs);
        const bool simpleMatch = getLeft()->equal(other->getLeft()) and getRight()->equal(other->getRight());
        const bool commutativeMatch = getLeft()->equal(other->getRight()) and getRight()->equal(other->getLeft());
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string AddBinaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *children[0] << "+" << *children[1];
    return ss.str();
}

std::shared_ptr<LogicalFunction> AddBinaryLogicalFunction::deepCopy()
{
    return LogicalFunctionAdd::create(
        Util::as<LogicalFunction>(children[0])->deepCopy(), Util::as<LogicalFunction>(children[1])->deepCopy());
}

}
