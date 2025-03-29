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
#include <Functions/ArithmeticalFunctions/FloorUnaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

FloorUnaryLogicalFunction::FloorUnaryLogicalFunction(std::shared_ptr<DataType> stamp) : UnaryLogicalFunction(std::move(stamp), "Floor") {};

FloorUnaryLogicalFunction::FloorUnaryLogicalFunction(FloorUnaryLogicalFunction* other) : UnaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction> FloorUnaryLogicalFunction::create(std::shared_ptr<LogicalFunction> const& child)
{
    auto floorNode = std::make_shared<FloorUnaryLogicalFunction>(child->getStamp());
    floorNode->setChild(child);
    return floorNode;
}

bool FloorUnaryLogicalFunction::equal(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<FloorUnaryLogicalFunction>(rhs))
    {
        auto otherFloorNode = NES::Util::as<FloorUnaryLogicalFunction>(rhs);
        return getChild()->equal(otherFloorNode->getChild());
    }
    return false;
}

std::string FloorUnaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "FLOOR(" << *getChild() << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> FloorUnaryLogicalFunction::clone() const
{
    return FloorUnaryLogicalFunction::create(Util::as<LogicalFunction>(getChild())->clone());
}
}
