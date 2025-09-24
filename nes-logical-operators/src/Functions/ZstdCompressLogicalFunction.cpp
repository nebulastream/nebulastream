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

#include <Functions/ZstdCompressLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

ZstdCompressLogicalFunction::ZstdCompressLogicalFunction(const LogicalFunction& child)
    : dataType(child.getDataType()), child(child)
{
}

bool ZstdCompressLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const ZstdCompressLogicalFunction*>(&rhs))
    {
        return child == other->child;
    }
    return false;
}

std::string ZstdCompressLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("ZSTD_COMPRESS({})", child.explain(verbosity));
}

DataType ZstdCompressLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction ZstdCompressLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction ZstdCompressLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> ZstdCompressLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction ZstdCompressLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.child = children[0];
    copy.dataType = children[0].getDataType();
    return copy;
};

std::string_view ZstdCompressLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction ZstdCompressLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterZstdCompressLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("ConcatLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return ZstdCompressLogicalFunction(arguments.children[0]);
}

}
