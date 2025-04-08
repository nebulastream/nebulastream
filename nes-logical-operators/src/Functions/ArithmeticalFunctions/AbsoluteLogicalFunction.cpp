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
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableFunction.pb.h>

namespace NES
{

AbsoluteLogicalFunction::AbsoluteLogicalFunction(LogicalFunction child) : stamp(child.getStamp().clone()), child(child)
{
}

AbsoluteLogicalFunction::AbsoluteLogicalFunction(const AbsoluteLogicalFunction& other) : stamp(other.stamp), child(other.child)
{
}

const DataType& AbsoluteLogicalFunction::getStamp() const
{
    return *stamp;
};

LogicalFunction AbsoluteLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

LogicalFunction AbsoluteLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return this->withChildren(newChildren);
};

std::vector<LogicalFunction> AbsoluteLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction AbsoluteLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.child = children[0];
    return *this;
};

std::string AbsoluteLogicalFunction::getType() const
{
    return std::string(NAME);
}

bool AbsoluteLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const AbsoluteLogicalFunction*>(&rhs);
    if (other)
    {
        return child == other->child;
    }
    return false;
}

std::string AbsoluteLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "ABS(" << child << ")";
    return ss.str();
}

SerializableFunction AbsoluteLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterAbsoluteLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return AbsoluteLogicalFunction(arguments.children[0]);
}

}
