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
#include <Functions/ArithmeticalFunctions/AbsoluteUnaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

AbsoluteUnaryLogicalFunction::AbsoluteUnaryLogicalFunction(std::shared_ptr<DataType> stamp)
    : UnaryLogicalFunction(std::move(stamp), "Abs") {};

AbsoluteUnaryLogicalFunction::AbsoluteUnaryLogicalFunction(AbsoluteUnaryLogicalFunction* other) : UnaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction> AbsoluteUnaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& child)
{
    auto absNode = std::make_shared<AbsoluteUnaryLogicalFunction>(child->getStamp());
    absNode->setChild(child);
    return absNode;
}

bool AbsoluteUnaryLogicalFunction::equal(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<AbsoluteUnaryLogicalFunction>(rhs))
    {
        auto otherAbsNode = NES::Util::as<AbsoluteUnaryLogicalFunction>(rhs);
        return child()->equal(otherAbsNode->child());
    }
    return false;
}

std::string AbsoluteUnaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "ABS(" << *child() << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> AbsoluteUnaryLogicalFunction::deepCopy()
{
    return AbsoluteUnaryLogicalFunction::create(Util::as<LogicalFunction>(child())->deepCopy());
}

}
