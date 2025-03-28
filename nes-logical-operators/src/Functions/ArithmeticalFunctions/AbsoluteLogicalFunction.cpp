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
#include <Functions/ArithmeticalFunctions/AbsoluteLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

AbsoluteLogicalFunction::AbsoluteLogicalFunction(std::shared_ptr<DataType> stamp) : UnaryLogicalFunction(std::move(stamp), "Abs") {};

AbsoluteLogicalFunction::AbsoluteLogicalFunction(AbsoluteLogicalFunction* other) : UnaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction> AbsoluteLogicalFunction::create(const std::shared_ptr<LogicalFunction>& child)
{
    auto absNode = std::make_shared<AbsoluteLogicalFunction>(child->getStamp());
    absNode->setChild(child);
    return absNode;
}

bool AbsoluteLogicalFunction::operator==(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<AbsoluteLogicalFunction>(rhs))
    {
        auto otherAbsNode = NES::Util::as<AbsoluteLogicalFunction>(rhs);
        return getChild()->equal(otherAbsNode->getChild());
    }
    return false;
}

std::string AbsoluteLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "ABS(" << *getChild() << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> AbsoluteLogicalFunction::clone() const
{
    return AbsoluteLogicalFunction::create(Util::as<LogicalFunction>(getChild())->clone());
}

}
