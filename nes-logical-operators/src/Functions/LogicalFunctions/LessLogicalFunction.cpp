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
#include <Functions/LogicalFunctions/LessLogicalFunction.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

LessLogicalFunction::LessLogicalFunction(const LessLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

LessLogicalFunction::LessLogicalFunction(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right) : BinaryLogicalFunction(std::move(stamp),"Less")
{
    this->setLeftChild(left);
    this->setRightChild(right);
}

bool LessLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<LessLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<LessLogicalFunction>(rhs);
        return this->getLeftChild()->equal(other->getLeftChild()) && this->getRightChild()->equal(other->getRightChild());
    }
    return false;
}

std::string LessLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << "<" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> LessLogicalFunction::clone() const
{
    return std::make_shared<LessLogicalFunction>(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

}
