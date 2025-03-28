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
#include <Functions/ArithmeticalFunctions/SubBinaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

SubBinaryLogicalFunction::SubBinaryLogicalFunction(std::shared_ptr<DataType> stamp)
    : LogicalFunctionArithmeticalBinary(std::move(stamp), "Sub") {};

SubBinaryLogicalFunction::SubBinaryLogicalFunction(SubBinaryLogicalFunction* other) : LogicalFunctionArithmeticalBinary(other)
{
}

std::shared_ptr<Logu>
SubBinaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto subNode = std::make_shared<SubBinaryLogicalFunction>(left->getStamp());
    subNode->setChildren(left, right);
    return subNode;
}

bool SubBinaryLogicalFunction::equal(std::shared_ptr<Operator> const& rhs) const
{
    if (NES::Util::instanceOf<SubBinaryLogicalFunction>(rhs))
    {
        auto otherSubNode = NES::Util::as<SubBinaryLogicalFunction>(rhs);
        return getLeft()->equal(otherSubNode->getLeft()) && getRight()->equal(otherSubNode->getRight());
    }
    return false;
}

std::string SubBinaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *children[0] << "-" << *children[1];
    return ss.str();
}

std::shared_ptr<LogicalFunction> SubBinaryLogicalFunction::deepCopy()
{
    return SubBinaryLogicalFunction::create(
        Util::as<LogicalFunction>(children[0])->deepCopy(), Util::as<LogicalFunction>(children[1])->deepCopy());
}

}
