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

#include <Configurations/ConfigField.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Pointers.hpp>
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

SourceDescriptor
OptimizerTestUtils::createSourceDescriptor(const Identifier& identifier, const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    auto source = sourceCatalog->addLogicalSource(identifier, schema);

    if (!source.has_value())
    {
        throw TestException();
    }
    const Schema<LiteralConfigValue, Ordered> values{
        LiteralConfigValue{QualifiedIdentifier::parse("FILE_PATH"), "/dev/null"},
        LiteralConfigValue{QualifiedIdentifier::parse("HOST"), "localhost"},
        LiteralConfigValue{QualifiedIdentifier::parse("TYPE"), "CSV"}};
    auto configSchema = SourceCatalog::getConfigSchema(Identifier::parse("file"), Identifier::parse("CSV")).value();
    auto [generalConfig, pluginConfig, inputFormatterDescriptor, declaredSchema] = configSchema.resolveConfigs(values).value();
    auto result = sourceCatalog->registerWithLogicalSource(
        PhysicalSourceBuilder{
            std::move(generalConfig), std::move(pluginConfig), std::move(inputFormatterDescriptor), copyPtr(sourceCatalog)},
        source->getLogicalSourceName());

    if (!result.has_value())
    {
        throw TestException();
    }
    return result.value();
}

SinkDescriptor OptimizerTestUtils::createSinkDescriptor(const Identifier& sinkName, const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    auto [generalConfig, sinkSchema, pluginSinkConfig, outputFormatterDescriptor]
        = SinkCatalog::getConfigSchema(Identifier::parse("file"), Identifier::parse("CSV"))
              .value()
              .resolveConfigs(Schema<LiteralConfigValue, Ordered>{std::vector<LiteralConfigValue>{
                  {QualifiedIdentifier::parse("FILE_SINK.FILE_PATH"), std::string{"/dev/null"}},
                  {QualifiedIdentifier::parse("OUTPUT_FORMATTER.TYPE"), std::string{"CSV"}}}})
              .value();

    generalConfig.host = Host{"localhost"};
    auto sinkDescriptor = sinkCatalog.addSinkDescriptor(
        sinkName, schema, std::move(generalConfig), std::move(pluginSinkConfig), std::move(outputFormatterDescriptor));
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
    return LogicalPlan{
        QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}), {std::move(sink)}};
}
}
