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
#include <Functions/ArithmeticalFunctions/NodeFunctionAbs.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

NodeFunctionAbs::NodeFunctionAbs(std::shared_ptr<DataType> stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Abs") {};

NodeFunctionAbs::NodeFunctionAbs(NodeFunctionAbs* other) : NodeFunctionArithmeticalUnary(other)
{
}

std::shared_ptr<NodeFunction> NodeFunctionAbs::create(const std::shared_ptr<NodeFunction>& child)
{
    auto absNode = std::make_shared<NodeFunctionAbs>(child->getStamp());
    absNode->setChild(child);
    return absNode;
}

bool NodeFunctionAbs::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionAbs>(rhs))
    {
        auto otherAbsNode = NES::Util::as<NodeFunctionAbs>(rhs);
        return child()->equal(otherAbsNode->child());
    }
    return false;
}

std::ostream& NodeFunctionAbs::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 1, "Cannot print function without exactly one child.");
    return os << fmt::format("ABS({})", *children.front());
}

std::shared_ptr<NodeFunction> NodeFunctionAbs::deepCopy()
{
    return NodeFunctionAbs::create(Util::as<NodeFunction>(children[0])->deepCopy());
}

}
