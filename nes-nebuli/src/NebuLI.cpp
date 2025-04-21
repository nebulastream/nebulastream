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
#include <ranges>
#include <regex>
#include <stdexcept>
#include <string>
#include <utility>
#include <API/Schema.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/QueryPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <SourceCatalogs/SourceCatalogEntry.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace YAML
{
using namespace NES::CLI;

std::unique_ptr<NES::DataType> stringToFieldType(const std::string& fieldNodeType)
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
        rhs.parserConfig = node["parserConfig"].as<std::unordered_map<std::string, std::string>>();
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

namespace NES::CLI
{

Sources::ParserConfig validateAndFormatParserConfig(const std::unordered_map<std::string, std::string>& parserConfig)
{
    auto validParserConfig = Sources::ParserConfig{};
    if (const auto parserType = parserConfig.find("type"); parserType != parserConfig.end())
    {
        validParserConfig.parserType = parserType->second;
    }
    else
    {
        throw InvalidConfigParameter("Parser configuration must contain: type");
    }
    if (const auto tupleDelimiter = parserConfig.find("tupleDelimiter"); tupleDelimiter != parserConfig.end())
    {
        validParserConfig.tupleDelimiter = tupleDelimiter->second;
    }
    else
    {
        NES_DEBUG("Parser configuration did not contain: tupleDelimiter, using default: \\n");
        validParserConfig.tupleDelimiter = "\n";
    }
    if (const auto fieldDelimiter = parserConfig.find("fieldDelimiter"); fieldDelimiter != parserConfig.end())
    {
        validParserConfig.fieldDelimiter = fieldDelimiter->second;
    }
    else
    {
        NES_DEBUG("Parser configuration did not contain: fieldDelimiter, using default: ,");
        validParserConfig.fieldDelimiter = ",";
    }
    return validParserConfig;
}

Sources::SourceDescriptor createSourceDescriptor(
    std::string logicalSourceName,
    Schema schema,
    const std::unordered_map<std::string, std::string>& parserConfig,
    std::unordered_map<std::string, std::string> sourceConfiguration)
{
    PRECONDITION(
        sourceConfiguration.contains(Configurations::SOURCE_TYPE_CONFIG),
        "Missing `Configurations::SOURCE_TYPE_CONFIG` in source configuration");
    auto sourceType = sourceConfiguration.at(Configurations::SOURCE_TYPE_CONFIG);
    NES_DEBUG("Source type is: {}", sourceType);

    auto validParserConfig = validateAndFormatParserConfig(parserConfig);
    auto validSourceConfig = Sources::SourceValidationProvider::provide(sourceType, std::move(sourceConfiguration));
    return Sources::SourceDescriptor(
        std::move(schema), std::move(logicalSourceName), sourceType, std::move(validParserConfig), std::move(validSourceConfig));
}

void validateAndSetSinkDescriptors(const QueryPlan& query, const QueryConfig& config)
{
    PRECONDITION(
        query.getSinkOperators().size() == 1,
        "NebulaStream currently only supports a single sink per query, but the query contains: {}",
        query.getSinkOperators().size());
    PRECONDITION(not config.sinks.empty(), fmt::format("Expects at least one sink in the query config!"));
    if (const auto sink = config.sinks.find(query.getSinkOperators().at(0)->sinkName); sink != config.sinks.end())
    {
        auto validatedSinkConfig = Sinks::SinkDescriptor::validateAndFormatConfig(sink->second.type, sink->second.config);
        query.getSinkOperators().at(0)->sinkDescriptor =
            std::make_unique<Sinks::SinkDescriptor>(sink->second.type, std::move(validatedSinkConfig), false);
    }
    else
    {
        throw UnknownSinkType(
            "Sinkname {} not specified in the configuration {}",
            query.getSinkOperators().front()->sinkName,
            fmt::join(std::views::keys(config.sinks), ","));
    }
}

std::unique_ptr<QueryPlan> createFullySpecifiedQueryPlan(const QueryConfig& config)
{
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();


    /// Add logical sources to the SourceCatalog to prepare adding physical sources to each logical source.
    for (auto& [logicalSourceName, schemaFields] : config.logical)
    {
        auto schema = Schema();
        NES_INFO("Adding logical source: {}", logicalSourceName);
        for (auto& [name, type] : schemaFields)
        {
            schema = schema.addField(name, type->clone());
        }
        sourceCatalog->addLogicalSource(logicalSourceName, schema);
    }

    /// Add physical sources to corresponding logical sources.
    for (auto [logicalSourceName, parserConfig, sourceConfig] : config.physical)
    {
        auto sourceDescriptor = createSourceDescriptor(
            logicalSourceName, sourceCatalog->getSchemaForLogicalSource(logicalSourceName), parserConfig, std::move(sourceConfig));
        sourceCatalog->addPhysicalSource(
            logicalSourceName,
            Catalogs::Source::SourceCatalogEntry::create(
                NES::PhysicalSource::create(std::move(sourceDescriptor)),
                sourceCatalog->getLogicalSource(logicalSourceName),
                INITIAL<WorkerId>));
    }

    auto query = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(config.query);
    auto logicalSourceExpansionRule = LegacyOptimizer::LogicalSourceExpansionRule::create(sourceCatalog);
    auto typeInference = LegacyOptimizer::TypeInferencePhase::create(sourceCatalog);
    auto originIdInferencePhase = LegacyOptimizer::OriginIdInferencePhase::create();

    validateAndSetSinkDescriptors(query, config);
    logicalSourceExpansionRule->apply(query);
    query = typeInference->performTypeInferenceQuery(query);
    query = originIdInferencePhase->execute(query);
    query = typeInference->performTypeInferenceQuery(query);

    NES_INFO("QEP:\n {}", query.toString());
    return query.clone();
}

std::unique_ptr<QueryPlan> loadFromYAMLFile(const std::filesystem::path& filePath)
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

SchemaField::SchemaField(std::string name, std::unique_ptr<NES::DataType> type) : name(std::move(name)), type(std::move(type))
{
}

std::unique_ptr<QueryPlan> loadFrom(std::istream& inputStream)
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
