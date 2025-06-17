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

#include <Functions/TemporalIntersectsFunction.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <fmt/format.h>
#include <iostream>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES {

TemporalIntersectsFunction::TemporalIntersectsFunction(
    LogicalFunction lon, LogicalFunction lat, LogicalFunction ts)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
    , lon(std::move(lon))
    , lat(std::move(lat)) 
    , ts(std::move(ts))
{
}

std::string TemporalIntersectsFunction::explain(ExplainVerbosity verbosity) const {
    return fmt::format("TEMPORAL_INTERSECTS({}, {}, {})", 
                      lon.explain(verbosity), 
                      lat.explain(verbosity), 
                      ts.explain(verbosity));
}

DataType TemporalIntersectsFunction::getDataType() const {
    return dataType;
}

LogicalFunction TemporalIntersectsFunction::withDataType(const DataType& dataType) const {
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction TemporalIntersectsFunction::withInferredDataType(const Schema& schema) const {
    return TemporalIntersectsFunction(
        lon.withInferredDataType(schema),
        lat.withInferredDataType(schema),
        ts.withInferredDataType(schema)
    );
}

std::vector<LogicalFunction> TemporalIntersectsFunction::getChildren() const {
    return {lon, lat, ts};
}

LogicalFunction TemporalIntersectsFunction::withChildren(const std::vector<LogicalFunction>& children) const {
    if (children.size() != 3) {
        throw std::runtime_error("TemporalIntersectsFunction requires exactly 3 children");
    }
    return TemporalIntersectsFunction(children[0], children[1], children[2]);
}

std::string_view TemporalIntersectsFunction::getType() const {
    return NAME;
}

SerializableFunction TemporalIntersectsFunction::serialize() const {
    SerializableFunction function;
    function.set_function_type(std::string(NAME));
    function.add_children()->CopyFrom(lon.serialize());
    function.add_children()->CopyFrom(lat.serialize());
    function.add_children()->CopyFrom(ts.serialize());
    DataTypeSerializationUtil::serializeDataType(dataType, function.mutable_data_type());
    return function;
}

bool TemporalIntersectsFunction::operator==(const LogicalFunctionConcept& rhs) const {
    std::cout << "TemporalIntersectsFunction::operator==" << std::endl;
    if (const auto* other = dynamic_cast<const TemporalIntersectsFunction*>(&rhs)) {
        return lon == other->lon && lat == other->lat && ts == other->ts;
    }
    return false;
}


NES::LogicalFunctionRegistryReturnType NES::LogicalFunctionGeneratedRegistrar::RegisterTemporalIntersectsLogicalFunction(NES::LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 3, "TemporalIntersectsFunction requires exactly three children, but got {}", arguments.children.size());
    return TemporalIntersectsFunction(arguments.children[0], arguments.children[1], arguments.children[2]);
}

}