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
#include "LevelFloatLogicalFunction.hpp"

namespace NES
{

LevelFloatLogicalFunction::LevelFloatLogicalFunction(const LogicalFunction& child) : dataType(child.getDataType()), child(child)
{
}

bool LevelFloatLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const LevelFloatLogicalFunction*>(&rhs))
    {
        return child == other->child and dataType == other->dataType;
    }
    return false;
}

std::string LevelFloatLogicalFunction::explain(const ExplainVerbosity verbosity) const
{
    return fmt::format("LEVELFLOAT({})", child.explain(verbosity));
}

DataType LevelFloatLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction LevelFloatLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction LevelFloatLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto copy = *this;
    copy.child = this->child.withInferredDataType(schema);
    copy.dataType = copy.child.getDataType();
    return copy;
};

std::vector<LogicalFunction> LevelFloatLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction LevelFloatLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
};

std::string_view LevelFloatLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction LevelFloatLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterLevelFloatLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.children.size() == 1, "LevelFloatLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    return LevelFloatLogicalFunction(arguments.children.at(0));
}

}
