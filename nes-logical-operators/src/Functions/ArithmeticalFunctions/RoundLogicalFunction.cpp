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

#include <cmath>
#include <memory>
#include <utility>
#include <Functions/ArithmeticalFunctions/RoundLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>

namespace NES
{

RoundLogicalFunction::RoundLogicalFunction(std::shared_ptr<DataType> stamp) : UnaryLogicalFunction(std::move(stamp), "Round") {};

RoundLogicalFunction::RoundLogicalFunction(RoundLogicalFunction* other) : UnaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction> RoundLogicalFunction::create(const std::shared_ptr<LogicalFunction>& child)
{
    auto roundNode = std::make_shared<RoundLogicalFunction>(child->getStamp());
    roundNode->setChild(child);
    return roundNode;
}

bool RoundLogicalFunction::operator==(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<RoundLogicalFunction>(rhs))
    {
        auto otherRoundNode = NES::Util::as<RoundLogicalFunction>(rhs);
        return getChild()->equal(otherRoundNode->getChild());
    }
    return false;
}

std::string RoundLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "ROUND(" << *getChild() << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> RoundLogicalFunction::clone() const
{
    return RoundLogicalFunction::create(Util::as<LogicalFunction>(getChild())->clone());
}
}
