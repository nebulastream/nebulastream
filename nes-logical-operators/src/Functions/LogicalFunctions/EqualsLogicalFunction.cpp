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
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctions/EqualsLogicalFunction.hpp>

#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES
{

EqualsLogicalFunction::EqualsLogicalFunction(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
    : BinaryLogicalFunction(DataTypeFactory::createBoolean(), "Equals")
{
    this->setLeftChild(left);
    this->setRightChild(right);
}

EqualsLogicalFunction::EqualsLogicalFunction(const EqualsLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

bool EqualsLogicalFunction::operator==(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<EqualsLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<EqualsLogicalFunction>(rhs);
        const bool simpleMatch = getLeftChild() == other->getLeftChild() and getRightChild() == other->getRightChild();
        const bool commutativeMatch = getLeftChild() == other->getRightChild() and getRightChild() == other->getLeftChild();
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string EqualsLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << "==" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> EqualsLogicalFunction::clone() const
{
    return std::make_shared<EqualsLogicalFunction>(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

bool EqualsLogicalFunction::validateBeforeLowering() const
{
    const auto childLeft = Util::as<LogicalFunction>(getLeftChild());
    const auto childRight = Util::as<LogicalFunction>(getRightChild());

    /// If one of the children has a stamp of type text, the other child must also have a stamp of type text
    if (NES::Util::instanceOf<VariableSizedDataType>(childLeft->getStamp())
        != NES::Util::instanceOf<VariableSizedDataType>(childRight->getStamp()))
    {
        return false;
    }

    return true;
}

}
