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

#include <Functions/LogicalFunctions/NegateUnaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

NegateUnaryLogicalFunction::NegateUnaryLogicalFunction() : UnaryLogicalFunction("Negate") {};

NegateUnaryLogicalFunction::NegateUnaryLogicalFunction(NegateUnaryLogicalFunction* other) : UnaryLogicalFunction(other)
{
}

bool NegateUnaryLogicalFunction::equal(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<NegateUnaryLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<NegateUnaryLogicalFunction>(rhs);
        return this->child()->equal(other->child());
    }
    return false;
}

std::string NegateUnaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "!" << *child();
    return ss.str();
}

std::shared_ptr<LogicalFunction> NegateUnaryLogicalFunction::create(std::shared_ptr<LogicalFunction> const& child)
{
    auto equals = std::make_shared<NegateUnaryLogicalFunction>();
    equals->setChild(child);
    return equals;
}

void NegateUnaryLogicalFunction::inferStamp(std::shared_ptr<Schema> schema)
{
    /// delegate stamp inference of children
    LogicalFunction::inferStamp(schema);
    /// check if children stamp is correct
    if (!child()->isPredicate())
    {
        throw CannotInferSchema(
            fmt::format("Negate Function Node: the stamp of child must be boolean, but was: {}", child()->getStamp()->toString()));
    }
}
std::shared_ptr<LogicalFunction> NegateUnaryLogicalFunction::clone() const
{
    return NegateUnaryLogicalFunction::create(Util::as<LogicalFunction>(getChild())->clone());
}

bool NegateUnaryLogicalFunction::validateBeforeLowering() const
{
    return NES::Util::instanceOf<Boolean>(Util::as<LogicalFunction>(child())->getStamp());
}

}
