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
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

AddLogicalFunction::AddLogicalFunction(std::shared_ptr<DataType> stamp)
    : BinaryLogicalFunction(std::move(stamp), "Add") {};

AddLogicalFunction::AddLogicalFunction(AddLogicalFunction* other) : BinaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction>
AddLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto addNode = std::make_shared<AddLogicalFunction>(left->getStamp());
    addNode->setLeftChild(left);
    addNode->setRightChild(right);
    return addNode;
}

bool AddLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<AddLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<AddLogicalFunction>(rhs);
        const bool simpleMatch = getLeftChild()->equal(other->getLeftChild()) and getRightChild()->equal(other->getRightChild());
        const bool commutativeMatch = getLeftChild()->equal(other->getRightChild()) and getRightChild()->equal(other->getLeftChild());
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string AddLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << "+" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> AddLogicalFunction::clone() const
{
    return AddLogicalFunction::create(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

}
