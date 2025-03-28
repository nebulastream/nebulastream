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

#include <Functions/ArithmeticalFunctions/CeilUnaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

CeilUnaryLogicalFunction::CeilUnaryLogicalFunction(std::shared_ptr<DataType> stamp) : UnaryLogicalFunction(std::move(stamp), "Ceil") {};

CeilUnaryLogicalFunction::CeilUnaryLogicalFunction(CeilUnaryLogicalFunction* other) : UnaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction> CeilUnaryLogicalFunction::create(std::shared_ptr<LogicalFunction> const& child)
{
    auto ceilNode = std::make_shared<CeilUnaryLogicalFunction>(child->getStamp());
    ceilNode->setChild(child);
    return ceilNode;
}

bool CeilUnaryLogicalFunction::equal(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<CeilUnaryLogicalFunction>(rhs))
    {
        auto otherCeilNode = NES::Util::as<CeilUnaryLogicalFunction>(rhs);
        return child()->equal(otherCeilNode->child());
    }
    return false;
}

std::string CeilUnaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "CEIL(" << *child() << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> CeilUnaryLogicalFunction::deepCopy()
{
    return CeilUnaryLogicalFunction::create(Util::as<LogicalFunction>(child())->deepCopy());
}
}
