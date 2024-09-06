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

#include <fstream>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <Services/QueryParsingService.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <SourceCatalogs/SourceCatalogEntry.hpp>
#include <Util/Logger/Logger.hpp>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <NebuLI.hpp>
#include <parser.hpp>

namespace NES::CLI
{
struct SchemaField
{
    std::string name;
    BasicType type;
};

struct LogicalSource
{
    std::string name;
    std::vector<SchemaField> schema;
};

struct PhysicalSource
{
    std::string logical;
    std::map<std::string, std::string> config;
};

struct QueryConfig
{
    std::string query;
    std::vector<LogicalSource> logical;
    std::vector<PhysicalSource> physical;
};
} /// namespace NES::CLI

namespace YAML
{
using namespace NES::CLI;
template <>
struct convert<SchemaField>
{
    static bool decode(const Node& node, SchemaField& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.type = *magic_enum::enum_cast<NES::BasicType>(node["type"].as<std::string>());
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
        rhs.config = node["config"].as<std::map<std::string, std::string>>();
        return true;
    }
};
template <>
struct convert<NES::CLI::QueryConfig>
{
    static bool decode(const Node& node, NES::CLI::QueryConfig& rhs)
    {
        rhs.logical = node["logical"].as<std::vector<NES::CLI::LogicalSource>>();
        rhs.physical = node["physical"].as<std::vector<NES::CLI::PhysicalSource>>();
        rhs.query = node["query"].as<std::string>();
        return true;
    }
};
} /// namespace YAML

namespace NES::CLI
{

SourceDescriptorPtr createSourceDescriptor(SchemaPtr schema, PhysicalSourceTypePtr physicalSourceType)
{
    auto logicalSourceName = physicalSourceType->getLogicalSourceName();
    auto physicalSourceName = physicalSourceType->getPhysicalSourceName();
    auto sourceType = physicalSourceType->getSourceType();
    /// TODO(#74) We removed almost all sources this needs to be updated
    NES_DEBUG(
        "PhysicalSourceConfig: create Actual source descriptor with physical source: {} {} ",
        physicalSourceType->toString(),
        magic_enum::enum_name(sourceType));

    switch (sourceType)
    {
        case SourceType::CSV_SOURCE: {
            auto csvSourceType = physicalSourceType->as<CSVSourceType>();
            return CSVSourceDescriptor::create(schema, csvSourceType, logicalSourceName, physicalSourceName);
        }
        case SourceType::TCP_SOURCE: {
            auto tcpSourceType = physicalSourceType->as<TCPSourceType>();
            return TCPSourceDescriptor::create(schema, tcpSourceType, logicalSourceName, physicalSourceName);
        }
        default: {
            NES_THROW_RUNTIME_ERROR("PhysicalSourceConfig:: source type " + physicalSourceType->getSourceTypeAsString() + " not supported");
        }
    }
}

PhysicalSourceTypePtr createPhysicalSourceType(
    const std::string& logicalName, const std::string& physicalName, const std::map<std::string, std::string>& sourceConfiguration)
{
    if (!sourceConfiguration.contains(Configurations::SOURCE_TYPE_CONFIG))
    {
        NES_THROW_RUNTIME_ERROR("Missing `type` in source configuration");
    }

    auto modifiedSourceConfiguration = sourceConfiguration;
    modifiedSourceConfiguration[Configurations::PHYSICAL_SOURCE_NAME_CONFIG] = physicalName;
    modifiedSourceConfiguration[Configurations::LOGICAL_SOURCE_NAME_CONFIG] = logicalName;

    return Configurations::PhysicalSourceTypeFactory::createFromString("", modifiedSourceConfiguration);
}

DecomposedQueryPlanPtr createFullySpecifiedQueryPlan(const QueryConfig& config)
{
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();

    for (const auto& [logicalName, schemaFields] : config.logical)
    {
        NES_INFO("Adding logical source: {}", logicalName);
        auto schema = Schema::create();
        for (const auto& [name, type] : schemaFields)
        {
            schema = schema->addField(name, type);
        }
        sourceCatalog->addLogicalSource(logicalName, std::move(schema));
    }

    for (size_t i = 0; const auto& [logicalName, config] : config.physical)
    {
        auto sourceType = createPhysicalSourceType(logicalName, fmt::format("{}_phy{}", logicalName, i++), config);
        sourceCatalog->addPhysicalSource(
            logicalName,
            Catalogs::Source::SourceCatalogEntry::create(
                NES::PhysicalSource::create(std::move(sourceType)), sourceCatalog->getLogicalSource(logicalName), INITIAL<WorkerId>));
    }

    auto cppCompiler = Compiler::CPPCompiler::create();
    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog);
    auto logicalSourceExpansionRule = NES::Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    auto typeInference = Optimizer::TypeInferencePhase::create(sourceCatalog);
    auto originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();
    auto memoryLayoutSelectionPhase = Optimizer::MemoryLayoutSelectionPhase::create(coordinatorConfig->optimizer.memoryLayoutPolicy);
    auto queryRewritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfig);
    auto query = query_plan_or_fail(parse(config.query));

    semanticQueryValidation->validate(query);

    logicalSourceExpansionRule->apply(query);
    typeInference->execute(query);
    originIdInferencePhase->execute(query);
    memoryLayoutSelectionPhase->execute(query);
    queryRewritePhase->execute(query);
    typeInference->execute(query);

    for (auto& sourceOperator : query->getSourceOperators())
    {
        if (auto sourceDescriptor = sourceOperator->getSourceDescriptor(); sourceDescriptor->instanceOf<LogicalSourceDescriptor>())
        {
            /// Fetch logical and physical source name in the descriptor
            auto logicalSourceName = sourceDescriptor->getLogicalSourceName();
            auto physicalSourceName = sourceDescriptor->getPhysicalSourceName();
            /// Iterate over all available physical sources
            bool foundPhysicalSource = false;
            for (const auto& entry : sourceCatalog->getPhysicalSources(logicalSourceName))
            {
                NES_DEBUG("Replacing LogicalSourceDescriptor {} - {}", logicalSourceName, physicalSourceName);
                if (auto physicalSourceType = entry->getPhysicalSource(); physicalSourceType->getPhysicalSourceName() == physicalSourceName)
                {
                    auto physicalDescriptor
                        = createSourceDescriptor(sourceDescriptor->getSchema(), physicalSourceType->getPhysicalSourceType());
                    NES_DEBUG("Replacement with: {}", physicalDescriptor->toString());
                    sourceOperator->setSourceDescriptor(physicalDescriptor);
                    foundPhysicalSource = true;
                    break;
                }
            }
            NES_ASSERT(
                foundPhysicalSource,
                "Logical source descriptor could not be replaced with the physical source descriptor: " + physicalSourceName);
        }
    }

    NES_INFO("QEP:\n {}", query->toString());
    NES_INFO("Sink Schema: {}", query->getRootOperators()[0]->getOutputSchema()->toString());
    return DecomposedQueryPlan::create(INITIAL<QueryId>, INITIAL<WorkerId>, query->getRootOperators());
}

DecomposedQueryPlanPtr loadFromFile(const std::filesystem::path& filePath)
{
    std::ifstream file(filePath);
    if (!file)
    {
        throw QueryDescriptionNotReadable(std::strerror(errno));
    }

    return loadFrom(file);
}

DecomposedQueryPlanPtr loadFrom(std::istream& inputStream)
{
    try
    {
        auto config = YAML::Load(inputStream).as<QueryConfig>();
        return createFullySpecifiedQueryPlan(config);
    }
    catch (const YAML::ParserException& pex)
    {
        throw QueryDescriptionNotParsable(fmt::format(": {}.", pex.what()));
    }
}
}