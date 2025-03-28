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
#include <string>
#include <Functions/LogicalFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

GreaterEqualsLogicalFunction::GreaterEqualsLogicalFunction(const GreaterEqualsLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

GreaterEqualsLogicalFunction::GreaterEqualsLogicalFunction(
    const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
    : BinaryLogicalFunction(DataTypeFactory::createBoolean(), "GreaterEquals")
{
    this->setLeftChild(left);
    this->setRightChild(right);
}

bool GreaterEqualsLogicalFunction::operator==(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<GreaterEqualsLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<GreaterEqualsLogicalFunction>(rhs);
        return this->getLeftChild() == other->getLeftChild() && this->getRightChild() == other->getRightChild();
    }
    return false;
}

std::string GreaterEqualsLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << ">=" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> GreaterEqualsLogicalFunction::clone() const
{
    return std::make_shared<GreaterEqualsLogicalFunction>(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}
}
