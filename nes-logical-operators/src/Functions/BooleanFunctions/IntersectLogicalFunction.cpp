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
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/BooleanFunctions/IntersectLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <MEOSWrapper.hpp>

namespace NES
{

IntersectLogicalFunction::IntersectLogicalFunction(LogicalFunction lon, LogicalFunction lat, LogicalFunction ts)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
    , lon(std::move(std::move(lon)))
    , lat(std::move(std::move(lat)))
    , ts(std::move(std::move(ts)))
{
}

DataType IntersectLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction IntersectLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> IntersectLogicalFunction::getChildren() const
{
    return {lon, lat, ts};
};

LogicalFunction IntersectLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 3, "IntersectLogicalFunction requires exactly three children, but got {}", children.size());
    auto copy = *this;
    copy.lon = children[0];
    copy.lat = children[1];
    copy.ts = children[2];
    return copy;
};

std::string_view IntersectLogicalFunction::getType() const
{
    return NAME;
}

bool IntersectLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const IntersectLogicalFunction*>(&rhs))
    {
        const bool simpleMatch = lon == other->lon and lat == other->lat and ts == other->ts;
        const bool commutativeMatch = lon == other->lat and lat == other->lon and ts == other->ts;
        return simpleMatch or commutativeMatch;
    }
    return false;
}


std::string IntersectLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("INTERSECT({}, {}, {})", lon.explain(verbosity), lat.explain(verbosity), ts.explain(verbosity));
}

LogicalFunction IntersectLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& node : getChildren())
    {
        newChildren.push_back(node.withInferredDataType(schema));
    }
    /// check if children dataType is correct - spatial coordinates should be numeric
    INVARIANT(
        lon.getDataType().isType(DataType::Type::FLOAT64), "the dataType of longitude child must be FLOAT64, but was: {}", lon.getDataType());
    INVARIANT(
        lat.getDataType().isType(DataType::Type::FLOAT64),
        "the dataType of latitude child must be FLOAT64, but was: {}",
        lat.getDataType());
    INVARIANT(
        ts.getDataType().isType(DataType::Type::UINT64),
        "the dataType of timestamp child must be UINT64, but was: {}",
        ts.getDataType());
    return this->withChildren(newChildren);
}

SerializableFunction IntersectLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(lon.serialize());
    serializedFunction.add_children()->CopyFrom(lat.serialize());
    serializedFunction.add_children()->CopyFrom(ts.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterIntersectLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 3, "IntersectLogicalFunction requires exactly three children, but got {}", arguments.children.size());
    return IntersectLogicalFunction(arguments.children[0], arguments.children[1], arguments.children[2]);
}


}
