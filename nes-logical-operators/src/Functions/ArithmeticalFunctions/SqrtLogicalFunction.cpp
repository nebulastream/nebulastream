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

#include <Functions/ArithmeticalFunctions/SqrtLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>

#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>

namespace NES
{

SqrtLogicalFunction::SqrtLogicalFunction(std::shared_ptr<DataType> stamp) : UnaryLogicalFunction(std::move(stamp), "Sqrt") {};

SqrtLogicalFunction::SqrtLogicalFunction(SqrtLogicalFunction* other) : UnaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction> SqrtLogicalFunction::create(const std::shared_ptr<LogicalFunction>& child)
{
    auto sqrtNode = std::make_shared<SqrtLogicalFunction>(child->getStamp());
    sqrtNode->setChild(child);
    return sqrtNode;
}

bool SqrtLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<SqrtLogicalFunction>(rhs))
    {
        auto otherSqrtNode = NES::Util::as<SqrtLogicalFunction>(rhs);
        return getChild()->equal(otherSqrtNode->getChild());
    }
    return false;
}

std::string SqrtLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "SQRT(" << *getChild() << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> SqrtLogicalFunction::clone() const
{
    return SqrtLogicalFunction::create(Util::as<LogicalFunction>(getChild())->clone());
}

}
