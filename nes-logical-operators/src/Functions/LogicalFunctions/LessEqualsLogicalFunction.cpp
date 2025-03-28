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

#include <Functions/LogicalFunctions/LessEqualsLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

LessEqualsLogicalFunction::LessEqualsLogicalFunction(const LessEqualsLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

LessEqualsLogicalFunction::LessEqualsLogicalFunction(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right) : BinaryLogicalFunction(DataTypeFactory::createBoolean(), "LessEquals")
{
    this->setLeftChild(left);
    this->setRightChild(right);
}

bool LessEqualsLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<LessEqualsLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<LessEqualsLogicalFunction>(rhs);
        return this->getLeftChild() == other->getLeftChild() && this->getRightChild() == other->getRightChild();
    }
    return false;
}

std::string LessEqualsLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << "<=" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> LessEqualsLogicalFunction::clone() const
{
    return std::make_shared<LessEqualsLogicalFunction>(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

}
