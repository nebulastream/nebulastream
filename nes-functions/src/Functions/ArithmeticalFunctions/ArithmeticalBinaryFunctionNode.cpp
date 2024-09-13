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

#include <utility>
#include <Functions/ArithmeticalFunctions/ArithmeticalBinaryFunctionNode.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

ArithmeticalBinaryFunctionNode::ArithmeticalBinaryFunctionNode(DataTypePtr stamp, std::string name)
    : BinaryFunctionNode(std::move(stamp), std::move(name)), ArithmeticalFunctionNode()
{
}
ArithmeticalBinaryFunctionNode::ArithmeticalBinaryFunctionNode(ArithmeticalBinaryFunctionNode* other) : BinaryFunctionNode(other)
{
}

/**
 * @brief The current implementation of type inference for arithmetical functions expects that both
 * operands of an arithmetical function have numerical stamps.
 * If this is valid we derived the joined stamp of the left and right operand.
 * (e.g., left:int8, right:int32 -> int32)
 * @param schema the current schema we use during type inference.
 */
void ArithmeticalBinaryFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer the stamps of the left and right child
    auto left = getLeft();
    auto right = getRight();
    left->inferStamp(schema);
    right->inferStamp(schema);

    /// both sub functions have to be numerical
    if (!left->getStamp()->isNumeric() || !right->getStamp()->isNumeric())
    {
        NES_THROW_RUNTIME_ERROR(
            "Error during stamp inference. Types need to be Numerical but Left was: {} Right was: {}",
            left->getStamp()->toString(),
            right->getStamp()->toString());
    }

    /// calculate the common stamp by joining the left and right stamp
    auto commonStamp = left->getStamp()->join(right->getStamp());

    /// check if the common stamp is defined
    if (commonStamp->isUndefined())
    {
        /// the common stamp was not valid -> in this case the common stamp is undefined.
        NES_THROW_RUNTIME_ERROR("{} is not supported by arithmetical functions", commonStamp->toString());
    }

    stamp = commonStamp;
    NES_TRACE("ArithmeticalBinaryFunctionNode: we assigned the following stamp: {}", toString());
}

bool ArithmeticalBinaryFunctionNode::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<ArithmeticalBinaryFunctionNode>(rhs))
    {
        auto otherAddNode = NES::Util::as<ArithmeticalBinaryFunctionNode>(rhs);
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::string ArithmeticalBinaryFunctionNode::toString() const
{
    return "ArithmeticalBinaryFunction()";
}

} /// namespace NES
