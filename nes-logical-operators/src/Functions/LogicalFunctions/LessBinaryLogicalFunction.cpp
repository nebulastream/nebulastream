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
#include <Functions/LogicalFunctions/LessBinaryLogicalFunction.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

LessBinaryLogicalFunction::LessBinaryLogicalFunction() : BinaryLogicalFunction(DataTypeFactory::createBoolean(), "Less")
{
}

LessBinaryLogicalFunction::LessBinaryLogicalFunction(LessBinaryLogicalFunction* other) : BinaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction>
LessBinaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto lessThen = std::make_shared<LessBinaryLogicalFunction>();
    lessThen->setLeftChild(left);
    lessThen->setRightChild(right);
    return lessThen;
}

bool LessBinaryLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<LessBinaryLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<LessBinaryLogicalFunction>(rhs);
        return this->getLeftChild()->equal(other->getLeftChild()) && this->getRightChild()->equal(other->getRightChild());
    }
    return false;
}

std::string LessBinaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << "<" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> LessBinaryLogicalFunction::clone() const
{
    return LessBinaryLogicalFunction::create(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

}
