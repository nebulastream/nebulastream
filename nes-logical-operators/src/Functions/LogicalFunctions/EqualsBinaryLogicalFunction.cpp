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
#include <sstream>
#include <Functions/LogicalFunctions/EqualsBinaryLogicalFunction.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES
{

EqualsBinaryLogicalFunction::EqualsBinaryLogicalFunction() noexcept : BinaryLogicalFunction("Equals")
{
}

EqualsBinaryLogicalFunction::EqualsBinaryLogicalFunction(EqualsBinaryLogicalFunction* other) : BinaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction>
EqualsBinaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto equals = std::make_shared<EqualsBinaryLogicalFunction>();
    equals->setChildren(left, right);
    return equals;
}

bool EqualsBinaryLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<EqualsBinaryLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<EqualsBinaryLogicalFunction>(rhs);
        const bool simpleMatch = getLeft()->equal(other->getLeft()) and getRight()->equal(other->getRight());
        const bool commutativeMatch = getLeft()->equal(other->getRight()) and getRight()->equal(other->getLeft());
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string EqualsBinaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *children[0] << "==" << *children[1];
    return ss.str();
}

std::shared_ptr<LogicalFunction> EqualsBinaryLogicalFunction::clone() const
{
    return EqualsBinaryLogicalFunction::create(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

bool EqualsBinaryLogicalFunction::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }

    const auto childLeft = Util::as<LogicalFunction>(children[0]);
    const auto childRight = Util::as<LogicalFunction>(children[1]);

    /// If one of the children has a stamp of type text, the other child must also have a stamp of type text
    if (NES::Util::instanceOf<VariableSizedDataType>(childLeft->getStamp())
        != NES::Util::instanceOf<VariableSizedDataType>(childRight->getStamp()))
    {
        return false;
    }

    return true;
}

}
