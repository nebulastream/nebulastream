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

#include <sstream>
#include <utility>
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

SubLogicalFunction::SubLogicalFunction(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right) : BinaryLogicalFunction(left->getStamp(), "Sub")
{
    this->setLeftChild(left);
    this->setRightChild(right);
};

SubLogicalFunction::SubLogicalFunction(const SubLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

bool SubLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<SubLogicalFunction>(rhs))
    {
        auto otherSubNode = NES::Util::as<SubLogicalFunction>(rhs);
        return getLeftChild() == otherSubNode->getLeftChild() and getRightChild() == otherSubNode->getRightChild();
    }
    return false;
}

std::string SubLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *children[0] << "-" << *children[1];
    return ss.str();
}

std::shared_ptr<LogicalFunction> SubLogicalFunction::clone() const
{
    return std::make_shared<SubLogicalFunction>(Util::as<LogicalFunction>(children[0])->clone(), Util::as<LogicalFunction>(children[1])->clone());
}

}