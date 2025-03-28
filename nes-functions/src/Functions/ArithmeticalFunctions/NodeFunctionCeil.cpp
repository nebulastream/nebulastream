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
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionCeil.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

NodeFunctionCeil::NodeFunctionCeil(std::shared_ptr<DataType> stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Ceil") { };

NodeFunctionCeil::NodeFunctionCeil(NodeFunctionCeil* other) : NodeFunctionArithmeticalUnary(other)
{
}

std::shared_ptr<NodeFunction> NodeFunctionCeil::create(const std::shared_ptr<NodeFunction>& child)
{
    auto ceilNode = std::make_shared<NodeFunctionCeil>(child->getStamp());
    ceilNode->setChild(child);
    return ceilNode;
}

bool NodeFunctionCeil::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionCeil>(rhs))
    {
        auto otherCeilNode = NES::Util::as<NodeFunctionCeil>(rhs);
        return child()->equal(otherCeilNode->child());
    }
    return false;
}

std::ostream& NodeFunctionCeil::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 1, "Cannot print function without exactly one child.");
    return os << fmt::format("CEIL({})", *children.front());
}

std::shared_ptr<NodeFunction> NodeFunctionCeil::deepCopy()
{
    return NodeFunctionCeil::create(Util::as<NodeFunction>(children[0])->deepCopy());
}
}
