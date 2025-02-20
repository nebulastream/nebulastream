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

#include <Functions/ArithmeticalFunctions/CeilLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

CeilLogicalFunction::CeilLogicalFunction(std::shared_ptr<LogicalFunction> const& child) : UnaryLogicalFunction(child->getStamp(), "Ceil")
{
    this->setChild(child);
};

CeilLogicalFunction::CeilLogicalFunction(const CeilLogicalFunction& other) : UnaryLogicalFunction(other)
{
}

bool CeilLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<CeilLogicalFunction>(rhs))
    {
        auto otherCeilNode = NES::Util::as<CeilLogicalFunction>(rhs);
        return getChild() == otherCeilNode->getChild();
    }
    return false;
}

std::string CeilLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "CEIL(" << *getChild() << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> CeilLogicalFunction::clone() const
{
    return std::make_shared<CeilLogicalFunction>(Util::as<LogicalFunction>(getChild())->clone());
}
}
