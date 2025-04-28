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
#include <Functions/ArithmeticalFunctions/NodeFunctionRound.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

NodeFunctionRound::NodeFunctionRound(std::shared_ptr<DataType> stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Round") { };

NodeFunctionRound::NodeFunctionRound(NodeFunctionRound* other) : NodeFunctionArithmeticalUnary(other)
{
}

std::shared_ptr<NodeFunction> NodeFunctionRound::create(const std::shared_ptr<NodeFunction>& child)
{
    auto roundNode = std::make_shared<NodeFunctionRound>(child->getStamp());
    roundNode->setChild(child);
    return roundNode;
}

bool NodeFunctionRound::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionRound>(rhs))
    {
        auto otherRoundNode = NES::Util::as<NodeFunctionRound>(rhs);
        return child()->equal(otherRoundNode->child());
    }
    return false;
}

std::ostream& NodeFunctionRound::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 1, "Cannot print function without exactly one child.");
    return os << fmt::format("ROUND({})", *children.front());
}

std::shared_ptr<NodeFunction> NodeFunctionRound::deepCopy()
{
    return NodeFunctionRound::create(Util::as<NodeFunction>(children[0])->deepCopy());
}
}
