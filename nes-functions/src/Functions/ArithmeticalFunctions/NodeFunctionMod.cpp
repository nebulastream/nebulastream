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

#include <sstream>
#include <API/DataType.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionMod.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

NodeFunctionMod::NodeFunctionMod(DataType stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Mod"){};

NodeFunctionMod::NodeFunctionMod(NodeFunctionMod* other) : NodeFunctionArithmeticalBinary(other)
{
}

NodeFunctionPtr NodeFunctionMod::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto addNode = std::make_shared<NodeFunctionMod>(uint64()); /// TODO: stamp should always be float, but is this the right way?
    addNode->setChildren(left, right);
    return addNode;
}

void NodeFunctionMod::inferStamp(Schema& schema)
{
    NodeFunctionArithmeticalBinary::inferStamp(schema);

    /// we know that both children must have been Integer, too
    auto leftAsInt = getLeft()->getStamp().getUnderlying<IntegerType>();
    auto rightAsInt = getRight()->getStamp().getUnderlying<IntegerType>();

    if (!leftAsInt || !rightAsInt)
    {
        throw TypeInferenceException("Modulo expects two integer operands");
    }
    /// TODO: This is not correct
    Range r;
    r.setMin(0, false);
    r.setMax(rightAsInt->range.getMax().first, false);
    stamp = IntegerType{r};
    ///NodeFunction do nothing if the stamp is of type undefined (from ArithmeticalBinary::inferSchema(typeInferencePhaseContext, schema);)
}

bool NodeFunctionMod::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionMod>(rhs))
    {
        auto otherAddNode = NES::Util::as<NodeFunctionMod>(rhs);
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::string NodeFunctionMod::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "%" << children[1]->toString();
    return ss.str();
}

NodeFunctionPtr NodeFunctionMod::deepCopy()
{
    return NodeFunctionMod::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

}
