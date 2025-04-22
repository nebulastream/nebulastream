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

#include <memory>
#include <ostream>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionSqrt.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

NodeFunctionSqrt::NodeFunctionSqrt(DataType stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Sqrt") {};

NodeFunctionSqrt::NodeFunctionSqrt(NodeFunctionSqrt* other) : NodeFunctionArithmeticalUnary(other)
{
}

std::shared_ptr<NodeFunction> NodeFunctionSqrt::create(const std::shared_ptr<NodeFunction>& child)
{
    auto sqrtNode = std::make_shared<NodeFunctionSqrt>(child->getStamp());
    sqrtNode->setChild(child);
    return sqrtNode;
}

bool NodeFunctionSqrt::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionSqrt>(rhs))
    {
        auto otherSqrtNode = NES::Util::as<NodeFunctionSqrt>(rhs);
        return child()->equal(otherSqrtNode->child());
    }
    return false;
}

std::ostream& NodeFunctionSqrt::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 1, "Cannot print function without exactly one child.");
    return os << fmt::format("SQRT({})", *children.front());
}

std::shared_ptr<NodeFunction> NodeFunctionSqrt::deepCopy()
{
    return NodeFunctionSqrt::create(Util::as<NodeFunction>(children[0])->deepCopy());
}

}
