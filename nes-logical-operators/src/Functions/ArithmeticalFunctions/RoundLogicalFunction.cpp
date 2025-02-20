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

RoundLogicalFunction::RoundLogicalFunction(const std::shared_ptr<LogicalFunction>& child) : UnaryLogicalFunction(child->getStamp(), "Round")
{
    this->setChild(child);
};

RoundLogicalFunction::RoundLogicalFunction(const RoundLogicalFunction& other) : UnaryLogicalFunction(other)
{
}

bool RoundLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<RoundLogicalFunction>(rhs))
    {
        auto otherRoundNode = NES::Util::as<RoundLogicalFunction>(rhs);
        return getChild() == otherRoundNode->getChild();
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
    return std::make_shared<RoundLogicalFunction>(Util::as<LogicalFunction>(getChild())->clone());
}
}
