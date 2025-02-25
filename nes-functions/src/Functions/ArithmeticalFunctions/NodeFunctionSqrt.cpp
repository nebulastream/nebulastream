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
#include <Functions/ArithmeticalFunctions/NodeFunctionSqrt.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>


namespace NES
{

NodeFunctionSqrt::NodeFunctionSqrt(DataTypePtr stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Sqrt") {};

NodeFunctionSqrt::NodeFunctionSqrt(NodeFunctionSqrt* other) : NodeFunctionArithmeticalUnary(other)
{
}

NodeFunctionPtr NodeFunctionSqrt::create(NodeFunctionPtr const& child)
{
    auto sqrtNode = std::make_shared<NodeFunctionSqrt>(child->getStamp());
    sqrtNode->setChild(child);
    return sqrtNode;
}

bool NodeFunctionSqrt::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionSqrt>(rhs))
    {
        auto otherSqrtNode = NES::Util::as<NodeFunctionSqrt>(rhs);
        return child()->equal(otherSqrtNode->child());
    }
    return false;
}

std::string NodeFunctionSqrt::toString() const
{
    std::stringstream ss;
    ss << "SQRT(" << *children[0] << ")";
    return ss.str();
}

NodeFunctionPtr NodeFunctionSqrt::deepCopy()
{
    return NodeFunctionSqrt::create(Util::as<NodeFunction>(children[0])->deepCopy());
}

}
