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
#include <regex>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
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
#include <Parser/SLTParser.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <nlohmann/detail/input/binary_reader.hpp>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <NebuLI.hpp>

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
    NES_DEBUG(
        "PhysicalSourceConfig: create Actual source descriptor with physical source: {} {} ",
        physicalSourceType->toString(),
        magic_enum::enum_name(sourceType));

    switch (sourceType)
    {
        case SourceType::CSV_SOURCE: {
            auto csvSourceType = physicalSourceType->as<CSVSourceType>();
            return CsvSourceDescriptor::create(schema, csvSourceType, logicalSourceName, physicalSourceName);
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
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    auto parsingService = QueryParsingService::create(jitCompiler);
    auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(parsingService);
    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog);
    auto logicalSourceExpansionRule = NES::Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    auto typeInference = Optimizer::TypeInferencePhase::create(sourceCatalog);
    auto originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();
    auto memoryLayoutSelectionPhase = Optimizer::MemoryLayoutSelectionPhase::create(coordinatorConfig->optimizer.memoryLayoutPolicy);
    auto queryRewritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfig);

    auto query = syntacticQueryValidation->validate(config.query);
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

DecomposedQueryPlanPtr loadFromYAMLFile(const std::filesystem::path& filePath)
{
    std::ifstream file(filePath);
    if (!file)
    {
        throw CouldNotReadQueryDescription(std::strerror(errno));
    }

    return loadFrom(file);
}

std::vector<DecomposedQueryPlanPtr> loadFromSLTFile(const std::filesystem::path& filePath, const std::string& testname)
{
    std::vector<DecomposedQueryPlanPtr> plans{};
    QueryConfig config{};
    SLTParser parser{};

    parser.registerSubstitutionRule(
        "SINK",
        [&](std::string& substitute)
        {
            static uint64_t queryNr = 0;
            auto resultFile = std::string(PATH_TO_BINARY_DIR) + "/test/result/" + testname + std::to_string(queryNr++) + ".csv";
            substitute = "sink(FileSinkDescriptor::create(\"" + resultFile + "\", \"CSV_FORMAT\", \"APPEND\"));";
        });

    parser.registerSubstitutionRule(
        "TESTDATA", [&](std::string& substitute) { substitute = std::string(PATH_TO_BINARY_DIR) + "/test/testdata"; });

    if (!parser.loadFile(filePath))
    {
        NES_FATAL_ERROR("Failed to parse test file: {}", filePath);
        return {};
    }

    /// We add new found sources to our config
    parser.registerOnSourceCallback(
        [&](const std::vector<std::string>& parameters)
        {
            NES::CLI::LogicalSource logicalSource;
            logicalSource.name = parameters[1];
            /// Read schema fields till reached last parameter
            for (size_t i = 2; i < parameters.size() - 1; i += 2)
            {
                NES::CLI::SchemaField schemaField;
                schemaField.type = *magic_enum::enum_cast<BasicType>(parameters[i]);
                schemaField.name = parameters[i + 1];
                logicalSource.schema.push_back(schemaField);
            }

            NES::CLI::PhysicalSource physicalSource;
            physicalSource.logical = logicalSource.name;
            physicalSource.config["type"] = "CSV_SOURCE";
            physicalSource.config["filePath"] = parameters.back();

            config.logical.push_back(logicalSource);
            config.physical.push_back(physicalSource);
        });

    const auto tmpSourceDir = std::string(PATH_TO_BINARY_DIR) + "/test/";
    parser.registerOnSourceInFileTuplesCallback(
        [&](const std::vector<std::string>& SourceParameters, const std::vector<std::string>& tuples)
        {
            static uint64_t sourceIndex = 0;

            NES::CLI::LogicalSource logicalSource;
            logicalSource.name = SourceParameters[1];
            /// Read schema fields till reached last parameter
            for (size_t i = 2; i < SourceParameters.size() - 1; i += 2)
            {
                NES::CLI::SchemaField schemaField;
                schemaField.type = *magic_enum::enum_cast<BasicType>(SourceParameters[i]);
                schemaField.name = SourceParameters[i + 1];
                logicalSource.schema.push_back(schemaField);
            }

            NES::CLI::PhysicalSource physicalSource;
            physicalSource.logical = logicalSource.name;
            physicalSource.config["type"] = "CSV_SOURCE";
            physicalSource.config["filePath"] = tmpSourceDir + testname + std::to_string(sourceIndex) + ".csv";

            /// Write the tuples to a tmp file
            std::ofstream sourceFile(tmpSourceDir + testname + std::to_string(sourceIndex) + ".csv");
            if (!sourceFile)
            {
                NES_FATAL_ERROR("Failed to open source file: {}", tmpSourceDir + testname + std::to_string(sourceIndex) + ".csv");
                return;
            }
            for (const auto& tuple : tuples)
            {
                sourceFile << tuple << std::endl;
            }

            config.logical.push_back(logicalSource);
            config.physical.push_back(physicalSource);
        });

    /// We create a new query plan from our config when finding a query
    parser.registerOnQueryCallback(
        [&](const std::string_view query)
        {
            config.query = query;
            try
            {
                auto plan = createFullySpecifiedQueryPlan(config);
                plans.emplace_back(plan);
            }
            catch (const std::exception& e)
            {
                NES_FATAL_ERROR("Failed to create query plan: {}", e.what());
                exit(-1);
            }
        });
    if (!parser.parse())
    {
        NES_FATAL_ERROR("Failed to parse test file: {}", filePath);
        return {};
    }

    return plans;
}

bool getResultOfQuery(const std::string& testName, uint64_t queryNr, std::vector<std::string>& resultData)
{
    std::ifstream resultFile(PATH_TO_BINARY_DIR "/test/result/" + testName + std::to_string(queryNr) + ".csv");
    if (!resultFile)
    {
        NES_FATAL_ERROR("Failed to open result file: {}", PATH_TO_BINARY_DIR "/test/result/" + testName + ".csv");
        return false;
    }

    std::string line;
    std::regex headerRegex(R"(.*\$.*:.*)");
    while (std::getline(resultFile, line))
    {
        /// Skip the header
        if (std::regex_match(line, headerRegex))
        {
            continue;
        }
        resultData.push_back(line);
    }
    return true;
}

bool checkResult(const std::filesystem::path& filePath, const std::string& testName, uint64_t testNr)
{
    SLTParser parser{};
    if (!parser.loadFile(filePath))
    {
        NES_FATAL_ERROR("Failed to parse test file: {}", filePath);
        return false;
    }

    bool same = true;
    std::string errorMessages;
    uint64_t queryNr = 1;

    parser.registerOnQueryResultTuplesCallback(
        [&](std::vector<std::string>&& resultLines)
        {
            if (queryNr == testNr)
            {
                std::vector<std::string> queryResult;
                if (!getResultOfQuery(testName, queryNr, queryResult))
                {
                    same = false;
                    return;
                }

                /// Remove commas from the result
                std::for_each(queryResult.begin(), queryResult.end(), [](std::string& s) { std::replace(s.begin(), s.end(), ',', ' '); });
                std::for_each(resultLines.begin(), resultLines.end(), [](std::string& s) { std::replace(s.begin(), s.end(), ',', ' '); });

                /// Check for equality - tuples may be in different order
                if (queryResult.size() != resultLines.size())
                {
                    errorMessages += "Result does not match for query: " + std::to_string(queryNr) + "\n";
                    errorMessages += "Result size does not match: expected " + std::to_string(resultLines.size()) + ", got "
                        + std::to_string(queryResult.size()) + "\n";
                    same = false;
                }

                std::sort(queryResult.begin(), queryResult.end());
                std::sort(resultLines.begin(), resultLines.end());

                std::cout << "Query result: " << std::endl;
                for (const auto& line : queryResult)
                {
                    std::cout << line << std::endl;
                }
                std::cout << "Expected result: " << std::endl;
                for (const auto& line : resultLines)
                {
                    std::cout << line << std::endl;
                }

                if (!std::equal(queryResult.begin(), queryResult.end(), resultLines.begin()))
                {
                    /// Print the difference
                    errorMessages += "Result does not match for query: " + std::to_string(queryNr) + "\n";
                    errorMessages += "Expected:\n";
                    for (const auto& line : resultLines)
                    {
                        errorMessages += line + "\n";
                    }
                    errorMessages += "Got:\n";
                    for (const auto& line : queryResult)
                    {
                        errorMessages += line + "\n";
                    }
                    same = false;
                }
            }
            queryNr++;
        });

    if (!parser.parse())
    {
        NES_FATAL_ERROR("Failed to parse test file: {}", filePath);
        return false;
    }

    if (!same)
    {
        NES_FATAL_ERROR("{}", errorMessages);
    }

    return same;
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
        auto exception = CouldNotParseQueryDescription();
        exception.what() += fmt::format(": {}.", pex.what());
        throw exception;
    }
}
} /// namespace NES::CLI
