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

#include <OptimizerTestUtils.hpp>

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Schema<UnboundFieldBase<1>, Ordered> OptimizerTestUtils::createSchema(const std::vector<std::string>& names)
{
    std::vector<UnqualifiedUnboundField> fields = {};
    fields.reserve(names.size());
    for (const auto& name : names)
    {
        fields.emplace_back(Identifier::parse(name), DataTypeProvider::provideDataType(DataType::Type::UINT64));
    }

    return Schema<UnqualifiedUnboundField, Ordered>{fields};
}

TypedLogicalOperator<SourceDescriptorLogicalOperator>
OptimizerTestUtils::createSource(std::string name, const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    auto descriptor = createSourceDescriptor(Identifier::parse(std::move(name)), schema);
    return SourceDescriptorLogicalOperator::create(std::move(descriptor));
}

TypedLogicalOperator<SourceDescriptorLogicalOperator>
OptimizerTestUtils::createSource(std::string name, const std::vector<std::string>& fieldNames)
{
    return createSource(std::move(name), createSchema(fieldNames));
}

/// NOLINTBEGIN(readability-convert-member-functions-to-static)
SourceDescriptor
OptimizerTestUtils::createSourceDescriptor(const Identifier& identifier, const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    const LogicalSource logicalSource{identifier, schema};
    const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("FILE_PATH"), "/dev/null"}};
    auto sourceDescriptor = SourceDescriptor::create(
        INITIAL_PHYSICAL_SOURCE_ID,
        logicalSource,
        Identifier::parse("File"),
        Host{"localhost"},
        sourceConfig,
        {{Identifier::parse("type"), "CSV"}});
    if (!sourceDescriptor.has_value()) {
        throw TestException();
    }
    return sourceDescriptor.value();
}

/// NOLINTEND(readability-convert-member-functions-to-static)

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
SinkDescriptor OptimizerTestUtils::createSinkDescriptor(const Identifier& sinkName, const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    const std::unordered_map<Identifier, std::string> sinkConfig{
        {Identifier::parse("FILE_PATH"), "/dev/null"}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}};

    auto sinkDescriptor
            = SinkDescriptor::createNamed(INVALID_SINK_ID, sinkName, schema, Identifier::parse("File"),
                                          Host{"localhost"}, sinkConfig, {});
    if (!sinkDescriptor.has_value())
    {
        throw TestException();
    }
    return sinkDescriptor.value();
}

TypedLogicalOperator<SinkLogicalOperator>
OptimizerTestUtils::createSink(LogicalOperator child, std::string name, const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    return SinkLogicalOperator::create(std::move(child), createSinkDescriptor(Identifier::parse(std::move(name)), schema));
}

TypedLogicalOperator<SinkLogicalOperator>
OptimizerTestUtils::createSink(LogicalOperator child, std::string name, const std::vector<std::string>& fieldNames)
{
    return createSink(std::move(child), std::move(name), createSchema(fieldNames));
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan OptimizerTestUtils::createPlan(LogicalOperator sink)
{
    return LogicalPlan{INVALID_QUERY_ID, {std::move(sink)}};
}
}
