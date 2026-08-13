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
#include <utility>
#include <vector>

#include <Configurations/ConfigLiteral.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkConfigSchema.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceConfigSchema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>

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
    return createSourceDescriptor(INITIAL_PHYSICAL_SOURCE_ID, logicalSource);
}

SourceDescriptor OptimizerTestUtils::createSourceDescriptor(PhysicalSourceId id, const LogicalSource& logicalSource)
{
    const auto configSchema = SourceConfigSchema::create(Identifier::parse("File"), Identifier::parse("CSV"));
    if (!configSchema)
    {
        throw TestException();
    }
    const Schema<LiteralConfigValue, Ordered> values{
        LiteralConfigValue{QualifiedIdentifier::parse("file_path"), "/dev/null"},
        LiteralConfigValue{QualifiedIdentifier::parse("host"), "localhost"},
        LiteralConfigValue{QualifiedIdentifier::parse("type"), "CSV"}};
    auto resolved = configSchema->resolveConfigs(values);
    if (!resolved)
    {
        throw TestException();
    }
    auto [general, plugin, formatter, declaredSchema] = std::move(*resolved);
    return PhysicalSourceBuilder{std::move(general), std::move(plugin), std::move(formatter)}
        .build(id, *logicalSource.getSchema(), logicalSource.getLogicalSourceName())
        .value();
}

SinkDescriptor OptimizerTestUtils::createAnonymousSinkDescriptor(SinkId id)
{
    const auto configSchema = SinkConfigSchema::create(Identifier::parse("File"), Identifier::parse("CSV"));
    if (!configSchema)
    {
        throw TestException();
    }
    const Schema<LiteralConfigValue, Ordered> values{
        LiteralConfigValue{QualifiedIdentifier::parse("file_path"), "/dev/null"},
        LiteralConfigValue{QualifiedIdentifier::parse("host"), "localhost"},
        LiteralConfigValue{QualifiedIdentifier::parse("OUTPUT_FORMATTER.TYPE"), "CSV"}};
    auto resolved = configSchema->resolveConfigs(values);
    if (!resolved)
    {
        throw TestException();
    }
    auto [general, schema, plugin, formatter] = std::move(*resolved);
    return SinkDescriptor::createAnonymous(id, std::move(schema), std::move(general), std::move(plugin), std::move(formatter));
}

/// NOLINTEND(readability-convert-member-functions-to-static)

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
SinkDescriptor OptimizerTestUtils::createSinkDescriptor(const Identifier& sinkName, const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    const auto sinkSchema = SinkConfigSchema::create(Identifier::parse("File"), Identifier::parse("CSV"));
    if (!sinkSchema)
    {
        throw TestException();
    }
    const Schema<LiteralConfigValue, Ordered> values{
        LiteralConfigValue{QualifiedIdentifier::parse("file_path"), "/dev/null"},
        LiteralConfigValue{QualifiedIdentifier::parse("host"), "localhost"},
        LiteralConfigValue{QualifiedIdentifier::parse("OUTPUT_FORMATTER.TYPE"), "CSV"}};
    auto resolved = sinkSchema->resolveConfigs(values);
    if (!resolved)
    {
        throw TestException();
    }
    auto [general, anonymousSchema, plugin, formatter] = std::move(*resolved);
    return SinkDescriptor::createNamed(INVALID_SINK_ID, sinkName, schema, std::move(general), std::move(plugin), std::move(formatter));
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
    return LogicalPlan{
        QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}), {std::move(sink)}};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan OptimizerTestUtils::createPlan(std::vector<LogicalOperator> sinks)
{
    return LogicalPlan{
        QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}), std::move(sinks)};
}
}
