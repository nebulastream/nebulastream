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
#include <Functions/ArithmeticalFunctions/ExpLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

ExpLogicalFunction::ExpLogicalFunction(std::shared_ptr<LogicalFunction> const& child) : UnaryLogicalFunction(child->getStamp(), "Exp")
{
    this->setChild(child);
};

ExpLogicalFunction::ExpLogicalFunction(const ExpLogicalFunction& other) : UnaryLogicalFunction(other)
{
}

bool ExpLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<ExpLogicalFunction>(rhs))
    {
        auto otherExpNode = NES::Util::as<ExpLogicalFunction>(rhs);
        return getChild() == otherExpNode->getChild();
    }
    return false;
}

std::string ExpLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "EXP(" << *getChild() << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> ExpLogicalFunction::clone() const
{
    return std::make_shared<ExpLogicalFunction>(Util::as<LogicalFunction>(getChild())->clone());
}
}
