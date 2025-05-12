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

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <istream>
#include <memory>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/ConfigurationsNames.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <LegacyOptimizer/LogicalSourceExpansionRule.hpp>
#include <LegacyOptimizer/OriginIdInferencePhase.hpp>
#include <LegacyOptimizer/SourceInferencePhase.hpp>
#include <LegacyOptimizer/TypeInferencePhase.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>

namespace
{
NES::DataType stringToFieldType(const std::string& fieldNodeType)
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
}

namespace YAML
{
using namespace NES::CLI;

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

namespace
{
NES::ParserConfig validateAndFormatParserConfig(const std::unordered_map<std::string, std::string>& parserConfig)
{
    auto validParserConfig = NES::ParserConfig{};
    if (const auto parserType = parserConfig.find("type"); parserType != parserConfig.end())
    {
        validParserConfig.parserType = parserType->second;
    }
    else
    {
        throw NES::InvalidConfigParameter("Parser configuration must contain: type");
    }
    if (const auto tupleDelimiter = parserConfig.find("tupleDelimiter"); tupleDelimiter != parserConfig.end())
    {
        /// TODO #651: Add full support for tuple delimiters that are larger than one byte.
        PRECONDITION(tupleDelimiter->second.size() == 1, "We currently do not support tuple delimiters larger than one byte.");
        validParserConfig.tupleDelimiter = tupleDelimiter->second;
    }
    else
    {
        NES_DEBUG("Parser configuration did not contain: tupleDelimiter, using default: \\n");
        validParserConfig.tupleDelimiter = '\n';
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

NES::SourceDescriptor createSourceDescriptor(
    const NES::LogicalSource& logicalSource,
    const NES::WorkerId workerId,
    const std::unordered_map<std::string, std::string>& parserConfig,
    std::unordered_map<std::string, std::string> sourceConfiguration,
    NES::SourceCatalog& sourceCatalog)
{
    PRECONDITION(
        sourceConfiguration.contains(NES::Configurations::SOURCE_TYPE_CONFIG),
        "Missing `Configurations::SOURCE_TYPE_CONFIG` in source configuration");
    const auto sourceType = sourceConfiguration.at(NES::Configurations::SOURCE_TYPE_CONFIG);
    const auto buffersInLocalPool = [](const std::unordered_map<std::string, std::string>& sourceConfig)
    {
        /// Initialize with invalid value and overwrite with configured value if given
        auto numberOfBuffersInLocalPool = NES::SourceDescriptor::INVALID_NUMBER_OF_BUFFERS_IN_LOCAL_POOL;

        if (const auto configuredNumberOfBuffersInLocalPool = sourceConfig.find(NES::Configurations::NUMBER_OF_BUFFERS_IN_LOCAL_POOL);
            configuredNumberOfBuffersInLocalPool != sourceConfig.end())
        {
            if (const auto customNumberOfBuffersInLocalPool = NES::Util::from_chars<int>(configuredNumberOfBuffersInLocalPool->second))
            {
                numberOfBuffersInLocalPool = customNumberOfBuffersInLocalPool.value();
            }
        }
        return numberOfBuffersInLocalPool;
    }(sourceConfiguration);

    const auto validParserConfig = validateAndFormatParserConfig(parserConfig);
    auto validSourceConfig = NES::Sources::SourceValidationProvider::provide(sourceType, std::move(sourceConfiguration));
    const auto physicalSourceOpt = sourceCatalog.addPhysicalSource(
        logicalSource, workerId, sourceType, buffersInLocalPool, std::move(validSourceConfig), validParserConfig);
    if (physicalSourceOpt)
    {
        return physicalSourceOpt.value();
    }
    throw NES::UnknownSource("{}", logicalSource.getLogicalSourceName());
}
}

namespace NES::CLI
{
static void validateAndSetSinkDescriptors(LogicalPlan& query, const QueryConfig& config)
{
    auto sinkOperators = getOperatorByType<SinkLogicalOperator>(query);
    PRECONDITION(
        sinkOperators.size() == 1,
        "NebulaStream currently only supports a single sink per query, but the query contains: {}",
        sinkOperators.size());
    PRECONDITION(not config.sinks.empty(), "Expects at least one sink in the query config!");
    if (const auto sink = config.sinks.find(sinkOperators.at(0).sinkName); sink != config.sinks.end())
    {
        auto validatedSinkConfig = Sinks::SinkDescriptor::validateAndFormatConfig(sink->second.type, sink->second.config);
        auto copy = sinkOperators.at(0);
        copy.sinkDescriptor = std::make_shared<Sinks::SinkDescriptor>(sink->second.type, std::move(validatedSinkConfig), false);
        auto replaceResult = replaceOperator(query, sinkOperators.at(0), copy);
        INVARIANT(replaceResult.has_value(), "replaceOperator failed");
        query = std::move(replaceResult.value());
    }
    else
    {
        throw UnknownSinkType(
            "Sinkname {} not specified in the configuration {}",
            sinkOperators.front().sinkName,
            fmt::join(std::views::keys(config.sinks), ","));
    }
}

std::unique_ptr<LogicalPlan> createFullySpecifiedQueryPlan(const QueryConfig& config)
{
    auto sourceCatalog = std::make_shared<SourceCatalog>();


    /// Add logical sources to the SourceCatalog to prepare adding physical sources to each logical source.
    for (const auto& [logicalSourceName, schemaFields] : config.logical)
    {
        auto schema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
        NES_INFO("Adding logical source: {}", logicalSourceName);
        for (const auto& [name, type] : schemaFields)
        {
            schema = schema.addField(name, type);
        }
        if (const auto created = sourceCatalog->addLogicalSource(logicalSourceName, schema); not created.has_value())
        {
            throw SourceAlreadyExists("{}", logicalSourceName);
        }
    }

    /// Add physical sources to corresponding logical sources.
    for (auto [logicalSourceName, parserConfig, sourceConfig] : config.physical)
    {
        if (const auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName); logicalSource.has_value())
        {
            auto sourceDescriptor
                = createSourceDescriptor(logicalSource.value(), INITIAL<WorkerId>, parserConfig, std::move(sourceConfig), *sourceCatalog);
        }
        else
        {
            throw UnknownSource("{}", logicalSourceName);
        }
    }

    auto queryplan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(config.query);

    const auto sourceLogicalExpansionPhase = LegacyOptimizer::LogicalSourceExpansionRule(sourceCatalog);
    const auto originIdInferencePhase = LegacyOptimizer::OriginIdInferencePhase();
    const auto sourceInferencePhase = LegacyOptimizer::SourceInferencePhase(sourceCatalog);
    const auto typeInferencePhase = LegacyOptimizer::TypeInferencePhase();

    validateAndSetSinkDescriptors(queryplan, config);

    sourceInferencePhase.apply(queryplan);
    sourceLogicalExpansionPhase.apply(queryplan);
    NES::LegacyOptimizer::TypeInferencePhase::apply(queryplan);
    NES::LegacyOptimizer::OriginIdInferencePhase::apply(queryplan);
    NES::LegacyOptimizer::TypeInferencePhase::apply(queryplan);

    return std::make_unique<LogicalPlan>(queryplan);
}

std::unique_ptr<LogicalPlan> loadFromYAMLFile(const std::filesystem::path& filePath)
{
    std::ifstream file(filePath);
    if (!file)
    {
        throw QueryDescriptionNotReadable(std::strerror(errno));
    }

    return loadFrom(file);
}

SchemaField::SchemaField(std::string name, const std::string& typeName) : SchemaField(std::move(name), stringToFieldType(typeName))
{
}

SchemaField::SchemaField(std::string name, NES::DataType type) : name(std::move(name)), type(std::move(std::move(type)))
{
}

std::unique_ptr<LogicalPlan> loadFrom(std::istream& inputStream)
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
