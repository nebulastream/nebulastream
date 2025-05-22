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
#include <string>
#include <utility>

#include <API/Schema.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Numeric.hpp>

namespace NES
{

NodeFunctionArithmeticalUnary::NodeFunctionArithmeticalUnary(std::shared_ptr<DataType> stamp, std::string name)
    : NodeFunctionUnary(std::move(stamp), std::move(name))
{
}

NodeFunctionArithmeticalUnary::NodeFunctionArithmeticalUnary(NodeFunctionArithmeticalUnary* other) : NodeFunctionUnary(other)
{
}

/**
 * @brief The current implementation of type inference for arithmetical functions expects that both
 * operands of an arithmetical function have numerical stamps.
 * If this is valid we derived the joined stamp of the left and right operand.
 * (e.g., left:int8, right:int32 -> int32)
 * @param schema the current schema we use during type inference.
 */
void NodeFunctionArithmeticalUnary::inferStamp(const Schema& schema)
{
    /// infer stamp of child
    auto child = this->child();
    child->inferStamp(schema);

    /// get stamp from child
    auto child_stamp = child->getStamp();
    if (!NES::Util::instanceOf<Numeric>(child_stamp))
    {
        throw CannotInferSchema(
            fmt::format("Error during stamp inference. Types need to be Numerical but child was: {}", child->getStamp()->toString()));
    }

    this->stamp = child_stamp;
    NES_TRACE("We assigned the following stamp: {}", *this);
}

bool NodeFunctionArithmeticalUnary::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionArithmeticalUnary>(rhs))
    {
        auto otherAddNode = NES::Util::as<NodeFunctionArithmeticalUnary>(rhs);
        return child()->equal(otherAddNode->child());
    }
    return false;
}

std::ostream& NodeFunctionArithmeticalUnary::toDebugString(std::ostream& os) const
{
    return os << "ArithmeticalUnaryFunction()";
}

bool NodeFunctionArithmeticalUnary::validateBeforeLowering() const
{
    if (children.size() != 1)
    {
        return false;
    }
    return NES::Util::instanceOf<Numeric>(Util::as<NodeFunction>(this->getChildren()[0])->getStamp());
}

}
