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
#include <Functions/ArithmeticalFunctions/ExpUnaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

ExpUnaryLogicalFunction::ExpUnaryLogicalFunction(std::shared_ptr<DataType> stamp) : UnaryLogicalFunction(std::move(stamp), "Exp") {};

ExpUnaryLogicalFunction::ExpUnaryLogicalFunction(ExpUnaryLogicalFunction* other) : UnaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction> ExpUnaryLogicalFunction::create(std::shared_ptr<LogicalFunction> const& child)
{
    auto expNode = std::make_shared<ExpUnaryLogicalFunction>(child->getStamp());
    expNode->setChild(child);
    return expNode;
}

bool ExpUnaryLogicalFunction::equal(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<ExpUnaryLogicalFunction>(rhs))
    {
        auto otherExpNode = NES::Util::as<ExpUnaryLogicalFunction>(rhs);
        return child()->equal(otherExpNode->child());
    }
    return false;
}

std::string ExpUnaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "EXP(" << *child() << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> ExpUnaryLogicalFunction::deepCopy()
{
    return ExpUnaryLogicalFunction::create(Util::as<LogicalFunction>(child())->deepCopy());
}
}
