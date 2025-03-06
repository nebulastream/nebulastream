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
#include <string>
#include <utility>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
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
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace YAML
{
using namespace NES::CLI;

std::shared_ptr<NES::DataType> stringToFieldType(const std::string& fieldNodeType)
{
    if (fieldNodeType == "VARSIZED")
    {
        return NES::DataTypeFactory::createVariableSizedData();
    }

    if (fieldNodeType == "BOOLEAN")
    {
        return NES::DataTypeFactory::createBoolean();
    }

    if (fieldNodeType == "INT8")
    {
        return NES::DataTypeFactory::createInt8();
    }

    if (fieldNodeType == "UINT8")
    {
        return NES::DataTypeFactory::createUInt8();
    }

    if (fieldNodeType == "INT16")
    {
        return NES::DataTypeFactory::createInt16();
    }

    if (fieldNodeType == "UINT16")
    {
        return NES::DataTypeFactory::createUInt16();
    }

    if (fieldNodeType == "INT32")
    {
        return NES::DataTypeFactory::createInt32();
    }

    if (fieldNodeType == "UINT32")
    {
        return NES::DataTypeFactory::createUInt32();
    }

    if (fieldNodeType == "INT64")
    {
        return NES::DataTypeFactory::createInt64();
    }

    if (fieldNodeType == "UINT64")
    {
        return NES::DataTypeFactory::createUInt64();
    }

    if (fieldNodeType == "FLOAT32")
    {
        return NES::DataTypeFactory::createFloat();
    }

    if (fieldNodeType == "FLOAT64")
    {
        return NES::DataTypeFactory::createDouble();
    }

    if (fieldNodeType == "CHAR")
    {
        return NES::DataTypeFactory::createChar();
    }

    throw NES::SLTWrongSchema("Found Invalid Logical Source Configuration. {} is not a proper Schema Field Type.", fieldNodeType);
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

namespace NES::CLI
{

Sources::InputFormatterConfig
validateAndFormatInputFormatterConfig(const std::unordered_map<std::string, std::string>& inputFormatterConfig)
{
    auto validInputFormatterConfig = Sources::InputFormatterConfig{};
    if (const auto inputFormatterType = inputFormatterConfig.find("type"); inputFormatterType != inputFormatterConfig.end())
    {
        validInputFormatterConfig.type = inputFormatterType->second;
    }
    else
    {
        throw InvalidConfigParameter("InputFormatter configuration must contain: type");
    }
    if (const auto inputFormatterIsAsync = inputFormatterConfig.find("isAsync"); inputFormatterIsAsync != inputFormatterConfig.end())
    {
        if (const auto isAsync = Util::from_chars<bool>(inputFormatterIsAsync->second); isAsync.has_value())
        {
            validInputFormatterConfig.isAsync = isAsync.value();
        }
        else
        {
            throw InvalidConfigParameter(
                "InputFormatter configuration contained isAsync field, but it was not a supported boolean value: {}",
                inputFormatterIsAsync->second);
        }
    }
    if (const auto tupleDelimiter = inputFormatterConfig.find("tupleDelimiter"); tupleDelimiter != inputFormatterConfig.end())
    {
        /// TODO #651: Add full support for tuple delimiters that are larger than one byte.
        PRECONDITION(tupleDelimiter->second.size() == 1, "We currently do not support tuple delimiters larger than one byte.");
        validInputFormatterConfig.tupleDelimiter = tupleDelimiter->second;
    }
    else
    {
        NES_DEBUG("InputFormatter configuration did not contain: tupleDelimiter, using default: \\n");
        validInputFormatterConfig.tupleDelimiter = '\n';
    }
    if (const auto fieldDelimiter = inputFormatterConfig.find("fieldDelimiter"); fieldDelimiter != inputFormatterConfig.end())
    {
        validInputFormatterConfig.fieldDelimiter = fieldDelimiter->second;
    }
    else
    {
        NES_DEBUG("InputFormatter configuration did not contain: fieldDelimiter, using default: ,");
        validInputFormatterConfig.fieldDelimiter = ",";
    }
    return validInputFormatterConfig;
}

Sources::SourceDescriptor createSourceDescriptor(
    std::string logicalSourceName,
    std::shared_ptr<Schema> schema,
    const std::unordered_map<std::string, std::string>& inputFormatterConfig,
    std::unordered_map<std::string, std::string> sourceConfiguration)
{
    PRECONDITION(
        sourceConfiguration.contains(Configurations::SOURCE_TYPE_CONFIG),
        "Missing `Configurations::SOURCE_TYPE_CONFIG` in source configuration");
    auto sourceType = sourceConfiguration.at(Configurations::SOURCE_TYPE_CONFIG);
    NES_DEBUG("Source type is: {}", sourceType);

    auto validInputFormatterConfig = validateAndFormatInputFormatterConfig(inputFormatterConfig);
    auto validSourceConfig = Sources::SourceValidationProvider::provide(sourceType, std::move(sourceConfiguration));
    return Sources::SourceDescriptor(
        std::move(schema), std::move(logicalSourceName), sourceType, std::move(validInputFormatterConfig), std::move(validSourceConfig));
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
        auto validatedSinkConfig = *Sinks::SinkDescriptor::validateAndFormatConfig(sink->second.type, sink->second.config);
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
            logicalSourceName, sourceCatalog->getSchemaForLogicalSource(logicalSourceName), inputFormatterConfig, std::move(sourceConfig));
        sourceCatalog->addPhysicalSource(
            logicalSourceName,
            Catalogs::Source::SourceCatalogEntry::create(
                NES::PhysicalSource::create(std::move(sourceDescriptor)),
                sourceCatalog->getLogicalSource(logicalSourceName),
                INITIAL<WorkerId>));
    }

    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog);
    auto logicalSourceExpansionRule = NES::Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    auto typeInference = Optimizer::TypeInferencePhase::create(sourceCatalog);
    auto originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();
    auto queryRewritePhase = Optimizer::QueryRewritePhase::create();

    auto query = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(config.query);

    validateAndSetSinkDescriptors(*query, config);
    semanticQueryValidation->validate(query); /// performs the first type inference

    logicalSourceExpansionRule->apply(query);
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
