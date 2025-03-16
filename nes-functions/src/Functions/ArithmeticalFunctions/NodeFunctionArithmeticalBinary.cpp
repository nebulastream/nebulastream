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
#include <string>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

NodeFunctionArithmeticalBinary::NodeFunctionArithmeticalBinary(DataType stamp, std::string name)
    : NodeFunctionBinary(std::move(stamp), std::move(name))
{
}
NodeFunctionArithmeticalBinary::NodeFunctionArithmeticalBinary(NodeFunctionArithmeticalBinary* other) : NodeFunctionBinary(other)
{
}

/**
 * @brief The current implementation of type inference for arithmetical functions expects that both
 * operands of an arithmetical function have numerical stamps.
 * If this is valid we derived the joined stamp of the left and right operand.
 * (e.g., left:int8, right:int32 -> int32)
 * @param schema the current schema we use during type inference.
 */
void NodeFunctionArithmeticalBinary::inferStamp(const Schema& schema)
{
    /// infer the stamps of the left and right child
    const auto left = getLeft();
    const auto right = getRight();
    left->inferStamp(schema);
    right->inferStamp(schema);

    /// both sub functions have to be numerical
    if (not left->getStamp().isNumeric() or not right->getStamp().isNumeric())
    {
        throw CannotInferSchema(fmt::format(
            "Error during stamp inference. Types need to be Numerical but Left was: {} Right was: {}",
            left->getStamp(),
            right->getStamp()));
    }

    /// calculate the common stamp by joining the left and right stamp
    const auto commonStamp = left->getStamp().join(right->getStamp());
    NES_DEBUG(
        "NodeFunctionArithmeticalBinary: the common stamp is: {} with left {} and right {}",
        commonStamp,
        left->getStamp(),
        right->getStamp());

    /// check if the common stamp is defined
    if (commonStamp.type == DataType::Type::UNDEFINED)
    {
        /// the common stamp was not valid -> in this case the common stamp is undefined.
        throw CannotInferSchema(fmt::format("{} is not supported by arithmetical functions", commonStamp));
    }

    stamp = commonStamp;
    NES_DEBUG("NodeFunctionArithmeticalBinary: we assigned the following stamp: {}", toString());
}

bool NodeFunctionArithmeticalBinary::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionArithmeticalBinary>(rhs))
    {
        auto otherAddNode = NES::Util::as<NodeFunctionArithmeticalBinary>(rhs);
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::string NodeFunctionArithmeticalBinary::toString() const
{
    return "ArithmeticalBinaryFunction()";
}

bool NodeFunctionArithmeticalBinary::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }
    return Util::as<NodeFunction>(this->getChildren()[0])->getStamp().isNumeric()
        && Util::as<NodeFunction>(this->getChildren()[1])->getStamp().isNumeric();
}


}
