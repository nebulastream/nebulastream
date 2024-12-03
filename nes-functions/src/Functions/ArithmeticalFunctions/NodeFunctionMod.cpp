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
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionMod.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

NodeFunctionMod::NodeFunctionMod(std::shared_ptr<DataType> stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Mod") {};

NodeFunctionMod::NodeFunctionMod(NodeFunctionMod* other) : NodeFunctionArithmeticalBinary(other)
{
}

std::shared_ptr<NodeFunction> NodeFunctionMod::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto addNode = std::make_shared<NodeFunctionMod>(DataTypeProvider::provideDataType(LogicalType::FLOAT32));
    addNode->setChildren(left, right);
    return addNode;
}

bool NodeFunctionMod::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionMod>(rhs))
    {
        auto otherAddNode = NES::Util::as<NodeFunctionMod>(rhs);
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::ostream& NodeFunctionMod::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 2, "Cannot print function without exactly 2 children.");
    return os << *children.at(0) << " % " << *children.at(1);
}

std::shared_ptr<NodeFunction> NodeFunctionMod::deepCopy()
{
    return NodeFunctionMod::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

}
