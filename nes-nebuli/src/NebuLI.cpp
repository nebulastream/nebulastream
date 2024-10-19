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

#include <algorithm>
#include <fstream>
#include <regex>

#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Parser/SLTParser.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <Services/QueryParsingService.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <SourceCatalogs/SourceCatalogEntry.hpp>
#include <Sources/SourceProvider.hpp>
#include <SourcesValidation/SourceRegistryValidation.hpp>
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

struct Sink
{
    std::string name;
    std::string type;
    std::unordered_map<std::string, std::string> config;
};

struct LogicalSource
{
    std::string name;
    std::vector<SchemaField> schema;
};

struct PhysicalSource
{
    std::string logical;
    std::unordered_map<std::string, std::string> config;
};

struct QueryConfig
{
    std::string query;
    std::unordered_map<std::string, Sink> sinks;
    std::vector<LogicalSource> logical;
    std::vector<PhysicalSource> physical;
};
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
        rhs.type = *magic_enum::enum_cast<NES::BasicType>(node["type"].as<std::string>());
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
        rhs.config = node["config"].as<std::unordered_map<std::string, std::string>>();
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

Sources::SourceDescriptor
createSourceDescriptor(std::string logicalSourceName, SchemaPtr schema, std::unordered_map<std::string, std::string>&& sourceConfiguration)
{
    if (!sourceConfiguration.contains(Configurations::SOURCE_TYPE_CONFIG))
    {
        NES_THROW_RUNTIME_ERROR("Missing `type` in source configuration");
    }
    auto sourceType = sourceConfiguration.at(Configurations::SOURCE_TYPE_CONFIG);
    NES_DEBUG("Source type is: {}", sourceType);

    /// We currently only support CSV
    auto inputFormat = Configurations::InputFormat::CSV;

    if (auto validConfig = Sources::SourceRegistryValidation::instance().create(sourceType, std::move(sourceConfiguration)))
    {
        return Sources::SourceDescriptor(
            std::move(schema), std::move(logicalSourceName), sourceType, inputFormat, std::move(*validConfig.value()));
    }
    throw UnknownSourceType(fmt::format("We don't support the source type: {}", sourceType));
}

void validateAndSetSinkDescriptor(const QueryPlan& query, const QueryConfig& config)
{
    PRECONDITION(
        query.getSinkOperators().size() == 1,
        fmt::format(
            "NebulaStream currently only supports a single sink per query, but the query contains: {}", query.getSinkOperators().size()));
    if (const auto sink = config.sinks.find(query.getSinkOperators().at(0)->sinkName); sink != config.sinks.end())
    {
        auto validatedSinkConfig = *Sinks::SinkDescriptor::validateAndFormatConfig(sink->second.type, sink->second.config);
        query.getSinkOperators().at(0)->sinkDescriptor
            = std::make_shared<Sinks::SinkDescriptor>(sink->second.type, std::move(validatedSinkConfig), false);
    }
}

DecomposedQueryPlanPtr createFullySpecifiedQueryPlan(const QueryConfig& config)
{
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
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
    for (auto [logicalSourceName, config] : config.physical)
    {
        auto sourceDescriptor
            = createSourceDescriptor(logicalSourceName, sourceCatalog->getSchemaForLogicalSource(logicalSourceName), std::move(config));
        sourceCatalog->addPhysicalSource(
            logicalSourceName,
            Catalogs::Source::SourceCatalogEntry::create(
                NES::PhysicalSource::create(std::move(sourceDescriptor)),
                sourceCatalog->getLogicalSource(logicalSourceName),
                INITIAL<WorkerId>));
    }

    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    auto parsingService = QueryParsingService::create(jitCompiler);
    auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(parsingService);
    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog);
    auto logicalSourceExpansionRule = NES::Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    auto typeInference = Optimizer::TypeInferencePhase::create(sourceCatalog);
    auto originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();
    auto queryRewritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfig);

    auto query = syntacticQueryValidation->validate(config.query);
    validateAndSetSinkDescriptor(*query, config);
    semanticQueryValidation->validate(query); /// performs the first type inference

    logicalSourceExpansionRule->apply(query);
    typeInference->performTypeInferenceQuery(query);

    originIdInferencePhase->execute(query);
    queryRewritePhase->execute(query);
    typeInference->performTypeInferenceQuery(query);

    NES_INFO("QEP:\n {}", query->toString());
    NES_INFO("Sink Schema: {}", query->getRootOperators()[0]->getOutputSchema()->toString());
    return DecomposedQueryPlan::create(INITIAL<QueryId>, INITIAL<WorkerId>, query->getRootOperators());
}

DecomposedQueryPlanPtr loadFromYAMLFile(const std::filesystem::path& filePath)
{
    std::ifstream file(filePath);
    if (!file)
    {
        throw QueryDescriptionNotReadable(std::strerror(errno));
    }

    return loadFrom(file);
}

std::vector<DecomposedQueryPlanPtr> loadFromSLTFile(const std::filesystem::path& filePath, const std::string& testname)
{
    std::vector<DecomposedQueryPlanPtr> plans{};
    QueryConfig config{};
    SLTParser::SLTParser parser{};

    parser.registerSubstitutionRule(
        {"SINK",
         [&](std::string& substitute)
         {
             /// For system level tests, a single file can hold arbitrary many tests. We need to generate a unique sink name for
             /// every test by counting up a static query number. We then emplace the unique sinks in the global (per test file) query config.
             static size_t currentQueryNumber = 0;
             const std::string sinkName = testname + "_" + std::to_string(currentQueryNumber++);
             const auto resultFile = std::string(PATH_TO_BINARY_DIR) + "/tests/result/" + sinkName + ".csv";
             auto sink = Sink{
                 sinkName,
                 "File",
                 {std::make_pair("inputFormat", "CSV"), std::make_pair("filePath", resultFile), std::make_pair("append", "false")}};
             config.sinks.emplace(sinkName, std::move(sink));
             substitute = "sink(\"" + sinkName + "\");";
         }});

    parser.registerSubstitutionRule(
        {"TESTDATA", [&](std::string& substitute) { substitute = std::string(PATH_TO_BINARY_DIR) + "/tests/testdata"; }});

    if (!parser.loadFile(filePath))
    {
        NES_FATAL_ERROR("Failed to parse test file: {}", filePath);
        return {};
    }

    /// We add new found sources to our config
    parser.registerOnCSVSourceCallback(
        [&](SLTParser::SLTParser::CSVSource&& source)
        {
            config.logical.emplace_back(LogicalSource{
                .name = source.name,
                .schema = [&source]()
                {
                    std::vector<SchemaField> schema;
                    for (const auto& field : source.fields)
                    {
                        schema.push_back({.name = field.name, .type = field.type});
                    }
                    return schema;
                }()});

            config.physical.emplace_back(
                PhysicalSource{.logical = source.name, .config = {{"type", "CSV"}, {"filePath", source.csvFilePath}}});
        });

    const auto tmpSourceDir = std::string(PATH_TO_BINARY_DIR) + "/tests/";
    parser.registerOnSLTSourceCallback(
        [&](SLTParser::SLTParser::SLTSource&& source)
        {
            static uint64_t sourceIndex = 0;

            config.logical.emplace_back(LogicalSource{
                .name = source.name,
                .schema = [&source]()
                {
                    std::vector<SchemaField> schema;
                    for (const auto& field : source.fields)
                    {
                        schema.push_back({.name = field.name, .type = field.type});
                    }
                    return schema;
                }()});

            config.physical.emplace_back(PhysicalSource{
                .logical = source.name,
                .config = {{"type", "CSV"}, {"filePath", tmpSourceDir + testname + std::to_string(sourceIndex) + ".csv"}}});


            /// Write the tuples to a tmp file
            std::ofstream sourceFile(tmpSourceDir + testname + std::to_string(sourceIndex) + ".csv");
            if (!sourceFile)
            {
                NES_FATAL_ERROR("Failed to open source file: {}", tmpSourceDir + testname + std::to_string(sourceIndex) + ".csv");
                return;
            }

            /// Write tuples to csv file
            for (const auto& tuple : source.tuples)
            {
                sourceFile << tuple << std::endl;
            }
        });

    /// We create a new query plan from our config when finding a query
    parser.registerOnQueryCallback(
        [&](SLTParser::SLTParser::Query&& query)
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
    try
    {
        parser.parse();
    }
    catch (const Exception&)
    {
        tryLogCurrentException();
        return {};
    }
    return plans;
}

bool getResultOfQuery(const std::string& testName, uint64_t queryNr, std::vector<std::string>& resultData)
{
    std::string resultFileName = PATH_TO_BINARY_DIR "/tests/result/" + testName + "_" + std::to_string(queryNr) + ".csv";
    std::ifstream resultFile(resultFileName);
    if (!resultFile)
    {
        NES_FATAL_ERROR("Failed to open result file: {}", resultFileName);
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

bool checkResult(const std::filesystem::path& testFilePath, const std::string& testName, uint64_t queryNr)
{
    SLTParser::SLTParser parser{};
    if (!parser.loadFile(testFilePath))
    {
        NES_FATAL_ERROR("Failed to parse test file: {}", testFilePath);
        return false;
    }

    bool same = true;
    std::string printMessages;
    std::string errorMessages;
    uint64_t seenResultTupleSections = 0;
    uint64_t seenQuerySections = 0;

    constexpr bool printOnlyDifferences = true;
    parser.registerOnResultTuplesCallback(
        [&seenResultTupleSections, &errorMessages, queryNr, testName, &same](SLTParser::SLTParser::ResultTuples&& resultLines)
        {
            /// Result section matches with query --> we found the expected result to check for
            if (seenResultTupleSections == queryNr)
            {
                /// 1. Get query result
                std::vector<std::string> queryResult;
                if (!getResultOfQuery(testName, seenResultTupleSections, queryResult))
                {
                    same = false;
                    return;
                }

                /// 2. We allow commas in the result and the expected result. To ensure they are equal we remove them from both
                std::for_each(queryResult.begin(), queryResult.end(), [](std::string& s) { std::replace(s.begin(), s.end(), ',', ' '); });
                std::for_each(resultLines.begin(), resultLines.end(), [](std::string& s) { std::replace(s.begin(), s.end(), ',', ' '); });

                /// 3. Store lines with line ids
                std::vector<std::pair<int, std::string>> originalResultLines;
                std::vector<std::pair<int, std::string>> originalQueryResult;

                originalResultLines.reserve(resultLines.size());
                for (size_t i = 0; i < resultLines.size(); ++i)
                {
                    originalResultLines.emplace_back(i + 1, resultLines[i]); /// store with 1-based index
                }

                originalQueryResult.reserve(queryResult.size());
                for (size_t i = 0; i < queryResult.size(); ++i)
                {
                    originalQueryResult.emplace_back(i + 1, queryResult[i]); /// store with 1-based index
                }

                /// 4. Check if line sizes match
                if (queryResult.size() != resultLines.size())
                {
                    errorMessages += "Result does not match for query: " + std::to_string(seenResultTupleSections) + "\n";
                    errorMessages += "Result size does not match: expected " + std::to_string(resultLines.size()) + ", got "
                        + std::to_string(queryResult.size()) + "\n";
                    same = false;
                }

                /// 5. Check if content match
                std::sort(queryResult.begin(), queryResult.end());
                std::sort(resultLines.begin(), resultLines.end());

                if (!std::equal(queryResult.begin(), queryResult.end(), resultLines.begin()))
                {
                    /// 6. Build error message
                    errorMessages += "Result does not match for query " + std::to_string(seenResultTupleSections) + ":\n";
                    errorMessages += "[#Line: Expected Result | #Line: Query Result]\n";

                    /// Maximum width of "Expected (Line #)"
                    size_t maxExpectedWidth = 0;
                    for (const auto& line : originalResultLines)
                    {
                        size_t currentWidth = std::to_string(line.first).length() + 2 + line.second.length();
                        maxExpectedWidth = std::max(currentWidth, maxExpectedWidth);
                    }

                    auto maxSize = std::max(originalResultLines.size(), originalQueryResult.size());
                    for (size_t i = 0; i < maxSize; ++i)
                    {
                        std::string expectedStr;
                        if (i < originalResultLines.size())
                        {
                            expectedStr = std::to_string(originalResultLines[i].first) + ": " + originalResultLines[i].second;
                        }
                        else
                        {
                            expectedStr = "(missing)";
                        }

                        std::string gotStr;
                        if (i < originalQueryResult.size())
                        {
                            gotStr = std::to_string(originalQueryResult[i].first) + ": " + originalQueryResult[i].second;
                        }
                        else
                        {
                            gotStr = "(missing)";
                        }

                        /// Check if they are different or if we are not filtering by differences
                        bool const areDifferent
                            = (i >= originalResultLines.size() || i >= originalQueryResult.size()
                               || originalResultLines[i].second != originalQueryResult[i].second);

                        if (areDifferent || !printOnlyDifferences)
                        {
                            /// Align the expected string by padding it to the maximum width
                            errorMessages += expectedStr;
                            errorMessages += std::string(maxExpectedWidth - expectedStr.length(), ' ');
                            errorMessages += " | ";
                            errorMessages += gotStr;
                            errorMessages += "\n";
                        }
                    }

                    same = false;
                }
            }
            seenResultTupleSections++;
        });

    parser.registerOnQueryCallback(
        [&seenQuerySections, queryNr, &printMessages](const std::string& query)
        {
            if (seenQuerySections == queryNr)
            {
                printMessages = query;
            }
            seenQuerySections++;
        });

    try
    {
        parser.parse();
    }
    catch (const Exception&)
    {
        tryLogCurrentException();
        return false;
    }

    /// spd logger cannot handle multiline prints with proper color and pattern. And as this is only for test runs we use stdout here.
    std::cout << "==============================================================" << '\n';
    std::cout << printMessages << '\n';
    std::cout << "==============================================================" << std::endl;
    if (!same)
    {
        std::cerr << errorMessages << std::endl;
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
        throw QueryDescriptionNotParsable(fmt::format(": {}.", pex.what()));
    }
}
}
