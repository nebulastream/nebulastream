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
#include <Functions/ArithmeticalFunctions/NodeFunctionMod.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
namespace NES
{

NodeFunctionMod::NodeFunctionMod(DataTypePtr stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Mod") {};

NodeFunctionMod::NodeFunctionMod(NodeFunctionMod* other) : NodeFunctionArithmeticalBinary(other)
{
}

NodeFunctionPtr NodeFunctionMod::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto addNode = std::make_shared<NodeFunctionMod>(
        DataTypeFactory::createFloat()); /// TODO: stamp should always be float, but is this the right way?
    addNode->setChildren(left, right);
    return addNode;
}

void NodeFunctionMod::inferStamp(SchemaPtr schema)
{
    NodeFunctionArithmeticalBinary::inferStamp(schema);

    if (stamp->isInteger())
    {
        /// we know that both children must have been Integer, too
        auto leftAsInt = DataType::as<Integer>(getLeft()->getStamp());
        auto rightAsInt = DataType::as<Integer>(getRight()->getStamp());

        /// determine the range of values that the result must be in:
        /// e.g. when calculating MOD(..., -128) the result will always be in range [-127, 127]. And no other INT8 divisor will yield a wider range than -128 (=INT8_MIN).
        int64_t range = std::max(std::abs(rightAsInt->lowerBound), std::abs(rightAsInt->upperBound)) - 1;

        /// further, we know that the result of MOD(X, ...) can not be larger/smaller than X:
        int64_t newLowerBound = std::max(-range, leftAsInt->lowerBound);
        int64_t newUpperBound = std::min(range, leftAsInt->upperBound);

        stamp = DataTypeFactory::copyTypeAndTightenBounds(stamp, newLowerBound, newUpperBound);
    }
    else if (stamp->isFloat())
    {
        /// children can be integer or float
        auto leftStamp = getLeft()->getStamp();
        auto rightStamp = getRight()->getStamp();

        /// target values
        double leftL, leftU, rightL, rightU;

        if (leftStamp->isFloat())
        {
            auto leftAsFloat = DataType::as<Float>(static_cast<DataTypePtr>(leftStamp));
            leftL = leftAsFloat->lowerBound;
            leftU = leftAsFloat->upperBound;
        }
        else if (leftStamp->isInteger())
        {
            auto leftAsInteger = DataType::as<Integer>(static_cast<DataTypePtr>(leftStamp));
            leftL = (double)leftAsInteger->lowerBound;
            leftU = (double)leftAsInteger->upperBound;
        }
        else
        {
            return;
        }

        if (rightStamp->isFloat())
        {
            auto rightAsFloat = DataType::as<Float>(static_cast<DataTypePtr>(rightStamp));
            rightL = rightAsFloat->lowerBound;
            rightU = rightAsFloat->upperBound;
        }
        else if (rightStamp->isInteger())
        {
            auto rightAsInteger = DataType::as<Integer>(static_cast<DataTypePtr>(rightStamp));
            rightL = (double)rightAsInteger->lowerBound;
            rightU = (double)rightAsInteger->upperBound;
        }
        else
        {
            return;
        }

        double range = std::max(std::abs(rightL), std::abs(rightU));
        double newLowerBound = std::max(-range, leftL);
        double newUpperBound = std::min(range, leftU);

        stamp = DataTypeFactory::copyTypeAndTightenBounds(stamp, newLowerBound, newUpperBound);
    }
    ///NodeFunction do nothing if the stamp is of type undefined (from ArithmeticalBinary::inferSchema(typeInferencePhaseContext, schema);)
}

bool NodeFunctionMod::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<NodeFunctionMod>())
    {
        auto otherAddNode = rhs->as<NodeFunctionMod>();
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
    return NodeFunctionMod::create(children[0]->as<FunctionNode>()->deepCopy(), children[1]->as<FunctionNode>()->deepCopy());
}

bool NodeFunctionMod::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }
    return this->getChildren()[0]->as<FunctionNode>()->getStamp()->isNumeric()
        && this->getChildren()[1]->as<FunctionNode>()->getStamp()->isNumeric();
}

}
