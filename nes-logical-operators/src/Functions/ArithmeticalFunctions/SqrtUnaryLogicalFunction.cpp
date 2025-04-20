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
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp>
#include <Functions/ArithmeticalFunctions/SqrtUnaryLogicalFunction.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>

namespace NES
{

SqrtUnaryLogicalFunction::SqrtUnaryLogicalFunction(std::shared_ptr<DataType> stamp) : UnaryLogicalFunction(std::move(stamp), "Sqrt") {};

SqrtUnaryLogicalFunction::SqrtUnaryLogicalFunction(SqrtUnaryLogicalFunction* other) : UnaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction> SqrtUnaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& child)
{
    auto sqrtNode = std::make_shared<SqrtUnaryLogicalFunction>(child->getStamp());
    sqrtNode->setChild(child);
    return sqrtNode;
}

bool SqrtUnaryLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<SqrtUnaryLogicalFunction>(rhs))
    {
        auto otherSqrtNode = NES::Util::as<SqrtUnaryLogicalFunction>(rhs);
        return child()->equal(otherSqrtNode->child());
    }
    return false;
}

std::string SqrtUnaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "SQRT(" << *child() << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> SqrtUnaryLogicalFunction::clone() const
{
    return SqrtUnaryLogicalFunction::create(Util::as<LogicalFunction>(getChild())->clone());
}

}
