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
#include <Functions/ArithmeticalFunctions/AbsoluteLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <SerializableFunction.pb.h>
#include <LogicalFunctionRegistry.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>

namespace NES
{

AbsoluteLogicalFunction::AbsoluteLogicalFunction(const std::shared_ptr<LogicalFunction>& child)
    : UnaryLogicalFunction(child->getStamp(), child)
{
}

AbsoluteLogicalFunction::AbsoluteLogicalFunction(const AbsoluteLogicalFunction& other) : UnaryLogicalFunction(other)
{
}

bool AbsoluteLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<AbsoluteLogicalFunction>(rhs))
    {
        auto otherAbsNode = NES::Util::as<AbsoluteLogicalFunction>(rhs);
        return getChild() == otherAbsNode->getChild();
    }
    return false;
}

std::string AbsoluteLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "ABS(" << *getChild() << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> AbsoluteLogicalFunction::clone() const
{
    return Util::as<LogicalFunction>(std::make_shared<AbsoluteLogicalFunction>(getChild())->clone());
}

SerializableFunction AbsoluteLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    auto* funcDesc = new SerializableFunction_UnaryFunction();
    auto* child = funcDesc->mutable_child();
    child->CopyFrom(getChild()->serialize());

    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

std::unique_ptr<LogicalFunctionRegistryReturnType>
LogicalFunctionGeneratedRegistrar::RegisterAbsoluteLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<AbsoluteLogicalFunction>(
        arguments.stamp, arguments.config);
}

}
