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

#include <NebuLI.hpp>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <istream>
#include <memory>
#include <optional>
#include <ranges>
#include <regex>
#include <stdexcept>
#include <string>
#include <utility>

#include <API/Schema.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterDescriptor.hpp>
#include <InputFormatters/InputFormatterValidationProvider.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <SourceCatalogs/SourceCatalogEntry.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace YAML
{
using namespace NES::CLI;

std::shared_ptr<NES::DataType> stringToFieldType(const std::string& fieldNodeType)
{
    try
    {
        return NES::DataTypeProvider::provideDataType(fieldNodeType);
    }
    catch (std::runtime_error& e)
    {
        throw NES::SLTWrongSchema("Found Invalid Logical Source Configuration. {} is not a proper Schema Field Type.", fieldNodeType);
    }
}

template <>
struct convert<SchemaField>
{
    static bool decode(const Node& node, SchemaField& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.type = stringToFieldType(node["type"].as<std::string>());
        return true;
    }
};
template <>
struct convert<NES::CLI::Sink>
{
    static bool decode(const Node& node, NES::CLI::Sink& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.type = node["type"].as<std::string>();
        rhs.config = node["config"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};
template <>
struct convert<NES::CLI::LogicalSource>
{
    static bool decode(const Node& node, NES::CLI::LogicalSource& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.schema = node["schema"].as<std::vector<SchemaField>>();
        return true;
    }
};
template <>
struct convert<NES::CLI::PhysicalSource>
{
    static bool decode(const Node& node, NES::CLI::PhysicalSource& rhs)
    {
        rhs.logical = node["logical"].as<std::string>();
        rhs.inputFormatterConfig = node["inputFormatterConfig"].as<std::unordered_map<std::string, std::string>>();
        rhs.sourceConfig = node["sourceConfig"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};
template <>
struct convert<NES::CLI::QueryConfig>
{
    static bool decode(const Node& node, NES::CLI::QueryConfig& rhs)
    {
        const auto sink = node["sink"].as<NES::CLI::Sink>();
        rhs.sinks.emplace(sink.name, sink);
        rhs.logical = node["logical"].as<std::vector<NES::CLI::LogicalSource>>();
        rhs.physical = node["physical"].as<std::vector<NES::CLI::PhysicalSource>>();
        rhs.query = node["query"].as<std::string>();
        return true;
    }
};
}

namespace
{
NES::Sources::SourceDescriptor createSourceDescriptor(
    std::string logicalSourceName, std::shared_ptr<NES::Schema> schema, std::unordered_map<std::string, std::string> sourceConfiguration)
{
    static constexpr auto TypeConfig = NES::Configurations::Names::getString<NES::Configurations::Names::Option::TYPE_CONFIG>();
    PRECONDITION(sourceConfiguration.contains(TypeConfig), "Missing `Configurations::TYPE_CONFIG` in source configuration");
    const auto sourceType = sourceConfiguration.at(TypeConfig);
    const auto numberOfBuffersInSourceLocalBufferPool = [](const std::unordered_map<std::string, std::string>& sourceConfig)
    {
        /// Initialize with invalid value and overwrite with configured value if given
        auto numSourceLocalBuffers = NES::Sources::SourceDescriptor::INVALID_NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL;

        if (const auto configuredNumSourceLocalBuffers = sourceConfig.find(
                NES::Configurations::Names::getString<NES::Configurations::Names::Option::NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL>());
            configuredNumSourceLocalBuffers != sourceConfig.end())
        {
            if (const auto customNumSourceLocalBuffers = NES::Util::from_chars<int>(configuredNumSourceLocalBuffers->second))
            {
                numSourceLocalBuffers = customNumSourceLocalBuffers.value();
            }
        }
        return numSourceLocalBuffers;
    }(sourceConfiguration);

    auto validSourceConfig = NES::Sources::SourceValidationProvider::provide(sourceType, std::move(sourceConfiguration));
    return NES::Sources::SourceDescriptor(
        std::move(schema), std::move(logicalSourceName), sourceType, numberOfBuffersInSourceLocalBufferPool, std::move(validSourceConfig));
}

NES::InputFormatters::InputFormatterDescriptor createInputFormatterDescriptor(
    std::shared_ptr<NES::Schema> schema, std::unordered_map<std::string, std::string> inputFormatterConfiguration)
{
    const auto hasSpanningTuple = [](const std::unordered_map<std::string, std::string>& inputFormatterConfiguration)
    {
        if (const auto hasSpanningTupleString = inputFormatterConfiguration.find("hasSpanningTuples");
            hasSpanningTupleString != inputFormatterConfiguration.end())
        {
            if (const auto hasSpanningTuplesBool = NES::Util::from_chars<bool>(hasSpanningTupleString->second))
            {
                return hasSpanningTuplesBool.value();
            }
        }
        throw NES::InvalidConfigParameter("Missing mandatory 'hasSpanningTuples' in configuration.");
    }(inputFormatterConfiguration);

    const auto inputFormatterType = [](const std::unordered_map<std::string, std::string>& inputFormatterConfiguration,
                                       const bool hasSpanningTuple,
                                       const NES::Schema& schema)
    {
        if (const auto type
            = inputFormatterConfiguration.find(NES::Configurations::Names::getString<NES::Configurations::Names::Option::TYPE_CONFIG>());
            type != inputFormatterConfiguration.end())
        {
            /// We currently only support spanning tuple support for native types without varsized fields
            if (type->second == "Native" and hasSpanningTuple)
            {
                if (std::ranges::any_of(
                        schema.getFieldNames(),
                        [&schema](const auto& fieldName)
                        {
                            return NES::Util::instanceOf<NES::VariableSizedDataType>(
                                schema.getFieldByName(fieldName).value()->getDataType());
                        }))
                {
                    throw NES::InvalidConfigParameter("Not supporting variable sized data for the internal format with spanning tuples.");
                }
            }
            return type->second;
        }
        throw NES::UnknownInputFormatterType("Missing mandatory input formatter type in configuration.");
    }(inputFormatterConfiguration, hasSpanningTuple, *schema);

    auto validInputFormatterConfig
        = NES::InputFormatters::InputFormatterValidationProvider::provide(inputFormatterType, std::move(inputFormatterConfiguration));
    return NES::InputFormatters::InputFormatterDescriptor(
        std::move(schema), inputFormatterType, hasSpanningTuple, std::move(validInputFormatterConfig));
}
}

namespace NES::CLI
{

void validateAndSetSinkDescriptors(const QueryPlan& query, const QueryConfig& config)
{
    PRECONDITION(
        query.getSinkOperators().size() == 1,
        "NebulaStream currently only supports a single sink per query, but the query contains: {}",
        query.getSinkOperators().size());
    PRECONDITION(not config.sinks.empty(), "Expects at least one sink in the query config!");
    if (const auto sink = config.sinks.find(query.getSinkOperators().at(0)->sinkName); sink != config.sinks.end())
    {
        auto validatedSinkConfig = Sinks::SinkDescriptor::validateAndFormatConfig(sink->second.type, sink->second.config);
        query.getSinkOperators().at(0)->sinkDescriptor
            = std::make_shared<Sinks::SinkDescriptor>(sink->second.type, std::move(validatedSinkConfig), false);
    }
    else
    {
        throw UnknownSinkType(
            "Sinkname {} not specified in the configuration {}",
            query.getSinkOperators().front()->sinkName,
            fmt::join(std::views::keys(config.sinks), ","));
    }
}

std::shared_ptr<DecomposedQueryPlan> createFullySpecifiedQueryPlan(const QueryConfig& config)
{
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();


    /// Add logical sources to the SourceCatalog to prepare adding physical sources to each logical source.
    for (const auto& [logicalSourceName, schemaFields] : config.logical)
    {
        auto schema = Schema::create();
        NES_INFO("Adding logical source: {}", logicalSourceName);
        for (const auto& [name, type] : schemaFields)
        {
            schema = schema->addField(name, type);
        }
        sourceCatalog->addLogicalSource(logicalSourceName, schema);
    }

    /// Add physical sources to corresponding logical sources.
    for (auto [logicalSourceName, inputFormatterConfig, sourceConfig] : config.physical)
    {
        auto sourceDescriptor = createSourceDescriptor(
            logicalSourceName, sourceCatalog->getSchemaForLogicalSource(logicalSourceName), std::move(sourceConfig));
        auto inputFormatterDescriptor
            = createInputFormatterDescriptor(sourceCatalog->getSchemaForLogicalSource(logicalSourceName), std::move(inputFormatterConfig));
        sourceCatalog->addPhysicalSource(
            logicalSourceName,
            Catalogs::Source::SourceCatalogEntry::create(
                NES::PhysicalSource::create(sourceDescriptor, inputFormatterDescriptor),
                sourceCatalog->getLogicalSource(logicalSourceName),
                INITIAL<WorkerId>));
    }

    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog);
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule(sourceCatalog);
    auto typeInference = Optimizer::TypeInferencePhase::create(sourceCatalog);
    auto originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();
    auto queryRewritePhase = Optimizer::QueryRewritePhase::create();

    auto query = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(config.query);

    validateAndSetSinkDescriptors(*query, config);
    semanticQueryValidation->validate(query); /// performs the first type inference

    logicalSourceExpansionRule.apply(query);
    typeInference->performTypeInferenceQuery(query);

    originIdInferencePhase->execute(query);
    queryRewritePhase->execute(query);
    typeInference->performTypeInferenceQuery(query);

    NES_INFO("QEP:\n {}", query->toString());
    NES_INFO("Sink Schema: {}", query->getRootOperators()[0]->getOutputSchema()->toString());
    return std::make_shared<DecomposedQueryPlan>(INITIAL<QueryId>, INITIAL<WorkerId>, query->getRootOperators());
}

std::shared_ptr<DecomposedQueryPlan> loadFromYAMLFile(const std::filesystem::path& filePath)
{
    std::ifstream file(filePath);
    if (!file)
    {
        throw QueryDescriptionNotReadable(std::strerror(errno));
    }

    return loadFrom(file);
}

SchemaField::SchemaField(std::string name, const std::string& typeName) : SchemaField(std::move(name), YAML::stringToFieldType(typeName))
{
}

SchemaField::SchemaField(std::string name, std::shared_ptr<NES::DataType> type) : name(std::move(name)), type(std::move(type))
{
}

std::shared_ptr<DecomposedQueryPlan> loadFrom(std::istream& inputStream)
{
    try
    {
        auto config = YAML::Load(inputStream).as<QueryConfig>();
        return createFullySpecifiedQueryPlan(config);
    }
    catch (const YAML::ParserException& pex)
    {
        throw QueryDescriptionNotParsable("{}", pex.what());
    }
}
}
