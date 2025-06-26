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
#include <SystestBinder.hpp>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDataProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <SystestSources/SystestSourceYAMLBinder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>
#include <SystestParser.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{

/// Helper class to model the two-step process of creating sinks in systest. We cannot sink descriptors directly from sink definitions, because
/// every query should write to a separate file sink, while being able to share the sink definitions with other queries.
class SLTSinkProvider
{
public:
    explicit SLTSinkProvider()
    {
        auto [iter, success] = sinkProviders.emplace(
            "CHECKSUM",
            [this](
                const std::string_view assignedSinkName, std::filesystem::path filePath) -> std::expected<Sinks::SinkDescriptor, Exception>
            {
                if (auto [iter, success] = sinkSchemas.try_emplace(std::string{assignedSinkName}, checksumSchema); !success)
                {
                    return std::unexpected{SinkAlreadyExists("{}", assignedSinkName)};
                }
                auto validatedSinkConfig = Sinks::SinkDescriptor::validateAndFormatConfig(
                    "Checksum", std::unordered_map<std::string, std::string>{{"filePath", filePath}});
                return Sinks::SinkDescriptor{std::string{"Checksum"}, std::move(validatedSinkConfig), false};
            });
        INVARIANT(success, "Failed to register checksum sink");
    }

    bool registerSink(const std::string& sinkType, const std::string_view sinkNameInFile, const Schema& schema)
    {
        auto [iter, success] = sinkProviders.emplace(
            sinkNameInFile,
            [this, schema, sinkType](
                const std::string_view assignedSinkName, std::filesystem::path filePath) -> std::expected<Sinks::SinkDescriptor, Exception>
            {
                if (auto [iter, success] = sinkSchemas.try_emplace(std::string{assignedSinkName}, schema); not success)
                {
                    return std::unexpected{SinkAlreadyExists("{}", assignedSinkName)};
                }

                std::unordered_map<std::string, std::string> config{{"filePath", std::move(filePath)}};
                if (sinkType == "File")
                {
                    config["inputFormat"] = "CSV";
                    config["append"] = "false";
                }

                auto validatedSinkConfig = Sinks::SinkDescriptor::validateAndFormatConfig(sinkType, std::move(config));
                return Sinks::SinkDescriptor{sinkType, std::move(validatedSinkConfig), false};
            });
        return success;
    }

    std::expected<Sinks::SinkDescriptor, Exception>
    createActualSink(const std::string& sinkNameInFile, const std::string_view assignedSinkName, const std::filesystem::path& filePath)
    {
        const auto sinkProviderIter = sinkProviders.find(sinkNameInFile);
        if (sinkProviderIter == sinkProviders.end())
        {
            throw UnknownSink("{}", sinkNameInFile);
        }
        return sinkProviderIter->second(std::string{assignedSinkName}, filePath);
    }

    [[nodiscard]] std::optional<Schema> getSinkSchema(const std::string& sinkNameInFile) const
    {
        const auto sinkSchemaIter = sinkSchemas.find(sinkNameInFile);
        if (sinkSchemaIter == sinkSchemas.end())
        {
            return std::nullopt;
        }
        return sinkSchemaIter->second;
    }

    static inline const Schema checksumSchema = []
    {
        auto checksumSinkSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
        checksumSinkSchema.addField("S$Count", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        checksumSinkSchema.addField("S$Checksum", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        return checksumSinkSchema;
    }();

private:
    /// TODO #951 Remove this map in the sink catalog PR
    std::unordered_map<std::string, Schema> sinkSchemas;
    std::unordered_map<std::string, std::function<std::expected<Sinks::SinkDescriptor, Exception>(std::string_view, std::filesystem::path)>>
        sinkProviders;
};

/// A Builder for Systest queries that matches the steps in which information is added.
/// Contains logic to extract some more information from the set fields, and to validate that all fields have been set.
class SystestQueryBuilder
{
    /// We could make all the fields just public and set them, but since some setters contain more complex logic, I wanted to keep access uniform.
    std::optional<TestName> testName;
    SystestQueryId queryIdInFile;
    std::optional<std::filesystem::path> testFilePath;
    std::optional<std::filesystem::path> workingDir;
    std::optional<std::string> queryDefinition;
    std::optional<std::expected<LogicalPlan, Exception>> boundPlan;
    std::optional<LogicalPlan> optimizedPlan;
    std::optional<std::unordered_map<SourceDescriptor, std::pair<std::filesystem::path, uint64_t>>> sourcesToFilePathsAndCounts;
    std::optional<Schema> sinkOutputSchema;
    std::optional<std::variant<std::vector<std::string>, ExpectedError>> expectedResultsOrError;
    std::optional<std::shared_ptr<std::vector<std::jthread>>> additionalSourceThreads;
    bool built = false;

public:
    /// Constructor from systestQueryId so it can be auto-constructed in std::unordered_map
    explicit SystestQueryBuilder(const SystestQueryId queryIdInFile) : queryIdInFile(queryIdInFile) { }

    SystestQueryId getSystemTestQueryId() const { return queryIdInFile; }
    void setExpectedResult(std::variant<std::vector<std::string>, ExpectedError> expectedResultsOrError)
    {
        this->expectedResultsOrError = std::move(expectedResultsOrError);
    }

    void setName(TestName testName) { this->testName = std::move(testName); }

    void setPaths(std::filesystem::path testFilePath, std::filesystem::path workingDir)
    {
        this->testFilePath = std::move(testFilePath);
        this->workingDir = std::move(workingDir);
    }

    void setAdditionalSourceThreads(std::shared_ptr<std::vector<std::jthread>> additionalSourceThreads)
    {
        this->additionalSourceThreads = std::move(additionalSourceThreads);
    }

    void setQuery(std::string queryDefinition, std::expected<LogicalPlan, Exception> boundPlan)
    {
        this->queryDefinition = std::move(queryDefinition);
        this->boundPlan = std::move(boundPlan);
    }

    std::expected<LogicalPlan, Exception> getBoundPlan() const { return boundPlan.value(); }
    void setOptimizedPlan(LogicalPlan optimizedPlan, const SLTSinkProvider& sinkProvider)
    {
        this->optimizedPlan = std::move(optimizedPlan);
        std::unordered_map<SourceDescriptor, std::pair<std::filesystem::path, uint64_t>> sourceNamesToFilepathAndCountForQuery;
        std::ranges::for_each(
            NES::getOperatorByType<NES::SourceDescriptorLogicalOperator>(*this->optimizedPlan),
            [&sourceNamesToFilepathAndCountForQuery](const SourceDescriptorLogicalOperator& logicalSourceOperator)
            {
                auto& entry = sourceNamesToFilepathAndCountForQuery[logicalSourceOperator.getSourceDescriptor()];
                if (const auto path = logicalSourceOperator.getSourceDescriptor().tryGetFromConfig<std::string>(std::string{"filePath"});
                    path.has_value())
                {
                    entry = {*path, entry.second + 1};
                }
                else
                {
                    NES_INFO(
                        "No file found for physical source {} for logical source {}",
                        logicalSourceOperator.getSourceDescriptor().getPhysicalSourceId(),
                        logicalSourceOperator.getSourceDescriptor().getLogicalSource().getLogicalSourceName());
                }
            });
        this->sourcesToFilePathsAndCounts = std::move(sourceNamesToFilepathAndCountForQuery);
        const auto sinkOperatorOpt = this->optimizedPlan->rootOperators.at(0).tryGet<SinkLogicalOperator>();
        INVARIANT(sinkOperatorOpt.has_value(), "The optimized plan should have a sink operator");
        INVARIANT(sinkOperatorOpt.value().sinkDescriptor != nullptr, "The root sink should have a sink descriptor");
        if (sinkOperatorOpt.value().sinkDescriptor->sinkType == "Checksum")
        {
            sinkOutputSchema = SLTSinkProvider::checksumSchema;
        }
        else
        {
            sinkOutputSchema = this->optimizedPlan->rootOperators.at(0).getOutputSchema();
            if (sinkOutputSchema != sinkProvider.getSinkSchema(sinkOperatorOpt.value().sinkName))
            {
                this->boundPlan
                    = std::unexpected{TestException("The inferred sink schema does not match the schema declared in the systest")};
            }
        }
    }

    SystestQuery build() &&
    {
        INVARIANT(not built, "Cannot build a SystestQuery twice");
        built = true;
        INVARIANT(testName.has_value(), "Test name has not been set");
        INVARIANT(testFilePath.has_value(), "Test file path has not been set");
        INVARIANT(workingDir.has_value(), "Working directory has not been set");
        INVARIANT(queryDefinition.has_value(), "Query definition has not been set");
        INVARIANT(boundPlan.has_value(), "Bound plan has not been set");
        INVARIANT(expectedResultsOrError.has_value(), "Expected results or error has not been set");
        if (boundPlan.value().has_value())
        {
            INVARIANT(
                optimizedPlan.has_value() && sourcesToFilePathsAndCounts.has_value() && sinkOutputSchema.has_value()
                    && additionalSourceThreads.has_value(),
                "Neither optimized plan nor an exception has been set");
            return SystestQuery{
                .testName = std::move(testName.value()),
                .queryIdInFile = queryIdInFile,
                .testFilePath = std::move(testFilePath.value()),
                .workingDir = std::move(workingDir.value()),
                .queryDefinition = std::move(queryDefinition.value()),
                .planInfoOrException = SystestQuery::PlanInfo{
                .queryPlan = std::move(optimizedPlan.value()),
                .sourcesToFilePathsAndCounts = std::move(sourcesToFilePathsAndCounts.value()),
                .sinkOutputSchema = sinkOutputSchema.value(),
                },
                .expectedResultsOrError = std::move(expectedResultsOrError.value()),
            .additionalSourceThreads = additionalSourceThreads.value()};
        }
        return SystestQuery{
            .testName = std::move(testName.value()),
            .queryIdInFile = queryIdInFile,
            .testFilePath = std::move(testFilePath.value()),
            .workingDir = std::move(workingDir.value()),
            .queryDefinition = std::move(queryDefinition.value()),
            .planInfoOrException = std::unexpected{boundPlan.value().error()},
            .expectedResultsOrError = std::move(expectedResultsOrError.value()),
            .additionalSourceThreads = std::make_shared<std::vector<std::jthread>>()};
    }
};

struct SystestBinder::Impl
{
    explicit Impl(std::filesystem::path workingDir, std::filesystem::path testDataDir, std::filesystem::path configDir)
        : workingDir(std::move(workingDir)), testDataDir(std::move(testDataDir)), configDir(std::move(configDir))
    {
    }

    std::vector<SystestQuery> loadOptimizeQueries(const TestFileMap& discoveredTestFiles)
    {
        /// This method could also be removed with the checks and loop put in the SystestExecutor, but it's an aesthetic choice.
        std::vector<SystestQuery> queries;
        uint64_t loadedFiles = 0;
        for (const auto& testfile : discoveredTestFiles | std::views::values)
        {
            std::cout << "Loading queries from test file: file://" << testfile.getLogFilePath() << '\n' << std::flush;
            try
            {
                for (auto testsForFile = loadOptimizeQueriesFromTestFile(testfile); auto& query : testsForFile)
                {
                    queries.emplace_back(std::move(query));
                }
                ++loadedFiles;
            }
            catch (const Exception& exception)
            {
                tryLogCurrentException();
                std::cerr << fmt::format("Loading test file://{} failed: {}\n", testfile.getLogFilePath(), exception.what());
            }
        }
        std::cout << "Loaded test files: " << loadedFiles << "/" << discoveredTestFiles.size() << '\n' << std::flush;
        if (loadedFiles != discoveredTestFiles.size())
        {
            std::cerr << "Could not load all test files. Terminating.\n" << std::flush;
            std::exit(1);
        }
        return queries;
    }

    std::vector<SystestQuery> loadOptimizeQueriesFromTestFile(const Systest::TestFile& testfile)
    {
        SLTSinkProvider sinkProvider;
        auto loadedSystests = loadFromSLTFile(testfile.file, testfile.name(), *testfile.sourceCatalog, sinkProvider);
        std::unordered_set<SystestQueryId> foundQueries;

        auto buildSystests = loadedSystests
            | std::views::filter(
                                 [&testfile](const auto& loadedQueryPlan)
                                 {
                                     return testfile.onlyEnableQueriesWithTestQueryNumber.empty()
                                         or testfile.onlyEnableQueriesWithTestQueryNumber.contains(loadedQueryPlan.getSystemTestQueryId());
                                 })
            | std::ranges::views::transform(
                                 [&testfile, &foundQueries, &sinkProvider](auto& systest)
                                 {
                                     foundQueries.insert(systest.getSystemTestQueryId());

                                     if (systest.getBoundPlan().has_value())
                                     {
                                         const NES::CLI::LegacyOptimizer optimizer{testfile.sourceCatalog};
                                         systest.setOptimizedPlan(optimizer.optimize(systest.getBoundPlan().value()), sinkProvider);
                                     }
                                     return std::move(systest).build();
                                 })
            | std::ranges::to<std::vector>();

        /// Warn about queries specified via the command line that were not found in the test file
        std::ranges::for_each(
            testfile.onlyEnableQueriesWithTestQueryNumber
                | std::views::filter([&foundQueries](const SystestQueryId testNumber) { return not foundQueries.contains(testNumber); }),
            [&testfile](const auto badTestNumber)
            {
                std::cerr << fmt::format(
                    "Warning: Query number {} specified via command line argument but not found in file://{}",
                    badTestNumber,
                    testfile.file.string());
            });

        return buildSystests;
    }

    /// NOLINTBEGIN(readability-function-cognitive-complexity)
    std::vector<SystestQueryBuilder> loadFromSLTFile(
        const std::filesystem::path& testFilePath,
        const std::string_view testFileName,
        SourceCatalog& sourceCatalog,
        SLTSinkProvider& sltSinkProvider)
    {
        uint64_t sourceIndex = 0;
        /// The bound test cases from this file.
        /// While SystestQueryID is just an integer, unordered maps provide a much easier and safer interface then vectors.
        std::unordered_map<SystestQueryId, SystestQueryBuilder> plans{};
        std::shared_ptr<std::vector<std::jthread>> sourceThreads = std::make_shared<std::vector<std::jthread>>();
        const std::unordered_map<SourceDescriptor, std::filesystem::path> generatedDataPaths{};
        SystestParser parser{};

        parser.registerSubstitutionRule(
            {.keyword = "TESTDATA", .ruleFunction = [&](std::string& substitute) { substitute = testDataDir; }});
        parser.registerSubstitutionRule({.keyword = "CONFIG", .ruleFunction = [&](std::string& substitute) { substitute = configDir; }});

        if (!parser.loadFile(testFilePath))
        {
            throw TestException("Could not successfully load test file://{}", testFilePath.string());
        }

        /// We create a map from sink names to their schema
        parser.registerOnSystestSinkCallback(
            [&](const SystestParser::SystestSink& sinkParsed)
            {
                Schema schema{Schema::MemoryLayoutType::ROW_LAYOUT};
                for (const auto& [type, name] : sinkParsed.fields)
                {
                    schema.addField(name, type);
                }
                sltSinkProvider.registerSink(sinkParsed.type, sinkParsed.name, schema);
            });

        parser.registerOnSystestLogicalSourceCallback(
            [&](const SystestParser::SystestLogicalSource& source)
            {
                Schema schema{Schema::MemoryLayoutType::ROW_LAYOUT};
                for (const auto& [type, name] : source.fields)
                {
                    schema.addField(name, type);
                }
                if (const auto logicalSource = sourceCatalog.addLogicalSource(source.name, schema); not logicalSource.has_value())
                {
                    throw SourceAlreadyExists("{}", source.name);
                }
            });

        /// We add new found sources to our config
        parser.registerOnSystestAttachSourceCallback(
            [&](SystestAttachSource attachSource)
            {
                attachSource.serverThreads = sourceThreads;

                /// Load physical source from file and overwrite logical source name with value from attach source
                const auto initialPhysicalSourceConfig = [](const std::string& logicalSourceName,
                                                            const std::string& sourceConfigPath,
                                                            const std::string& inputFormatterConfigPath)
                {
                    try
                    {
                        auto loadedPhysicalSourceConfig = SystestSourceYAMLBinder::loadSystestPhysicalSourceFromYAML(
                            logicalSourceName, sourceConfigPath, inputFormatterConfigPath);
                        return loadedPhysicalSourceConfig;
                    }
                    catch (const std::exception& e)
                    {
                        throw CannotLoadConfig("Failed to parse source: {}", e.what());
                    }
                }(attachSource.logicalSourceName, attachSource.sourceConfigurationPath, attachSource.inputFormatterConfigurationPath);

                const auto [logical, parserConfig, sourceConfig] = [&]()
                {
                    switch (attachSource.testDataIngestionType)
                    {
                        case TestDataIngestionType::INLINE: {
                            if (attachSource.tuples.has_value())
                            {
                                const auto sourceFile = SystestQuery::sourceFile(workingDir, testFileName, sourceIndex++);
                                return Sources::SourceDataProvider::provideInlineDataSource(
                                    initialPhysicalSourceConfig, attachSource, sourceFile);
                            }
                            throw CannotLoadConfig("An InlineData source must have tuples, but tuples was null.");
                        }
                        case TestDataIngestionType::FILE: {
                            return Sources::SourceDataProvider::provideFileDataSource(
                                initialPhysicalSourceConfig, attachSource, testDataDir);
                        }
                        case TestDataIngestionType::GENERATOR: {
                            return Sources::SourceDataProvider::provideGeneratorDataSource(initialPhysicalSourceConfig, attachSource);
                        }
                    }
                    std::unreachable();
                }();
                if (const auto logicalSource = sourceCatalog.getLogicalSource(attachSource.logicalSourceName))
                {
                    if (const auto sourceDescriptor = sourceCatalog.addPhysicalSource(
                            logicalSource.value(),
                            INITIAL<WorkerId>,
                            attachSource.sourceType,
                            -1,
                            Sources::SourceValidationProvider::provide(attachSource.sourceType, sourceConfig),
                            ParserConfig::create(parserConfig)))
                    {
                        return;
                    }
                    throw UnknownSource(
                        "Failed to attach physical source with type {} to logical source {}",
                        attachSource.sourceType,
                        attachSource.logicalSourceName);
                }
                throw UnknownSource("Failed to attach physical source to logical source: {}", attachSource.logicalSourceName);
            });

        /// We create a new query plan from our config when finding a query
        parser.registerOnQueryCallback(
            [&](std::string query, const SystestQueryId currentQueryNumberInTest)
            {
                /// We have to get all sink names from the query and then create custom paths for each sink.
                /// The filepath can not be the sink name, as we might have multiple queries with the same sink name, i.e., sink20Booleans in FunctionEqual.test
                /// We assume:
                /// - the INTO keyword is the last keyword in the query
                /// - the sink name is the last word in the INTO clause
                const auto sinkName = [&query]() -> std::string
                {
                    const auto intoClause = query.find("INTO");
                    if (intoClause == std::string::npos)
                    {
                        NES_ERROR("INTO clause not found in query: {}", query);
                        return "";
                    }
                    const auto intoLength = std::string("INTO").length();
                    auto trimmedSinkName = std::string(Util::trimWhiteSpaces(query.substr(intoClause + intoLength)));

                    /// As the sink name might have a semicolon at the end, we remove it
                    if (trimmedSinkName.back() == ';')
                    {
                        trimmedSinkName.pop_back();
                    }
                    return trimmedSinkName;
                }();

                /// Replacing the sinkName with the created unique sink name
                const auto sinkForQuery = sinkName + std::to_string(currentQueryNumberInTest.getRawValue());
                query = std::regex_replace(query, std::regex(sinkName), sinkForQuery);

                /// Adding the sink to the sink config, such that we can create a fully specified query plan
                const auto resultFile = SystestQuery::resultFile(workingDir, testFileName, currentQueryNumberInTest);

                auto& currentTest = plans.emplace(currentQueryNumberInTest, currentQueryNumberInTest).first->second;
                auto sinkExpected = sltSinkProvider.createActualSink(sinkName, sinkForQuery, resultFile);
                if (not sinkExpected.has_value())
                {
                    currentTest.setQuery(std::move(query), std::unexpected(sinkExpected.error()));
                }
                else
                {
                    try
                    {
                        auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
                        auto sinkOperators = plan.rootOperators;
                        auto sinkOperator = [](const LogicalPlan& queryPlan)
                        {
                            const auto rootOperators = queryPlan.rootOperators;
                            if (rootOperators.size() != 1)
                            {
                                throw QueryInvalid(
                                    "NebulaStream currently only supports a single sink per query, but the query contains: {}",
                                    rootOperators.size());
                            }
                            const auto sinkOp = rootOperators.at(0).tryGet<SinkLogicalOperator>();
                            INVARIANT(sinkOp.has_value(), "Root operator in plan was not sink");
                            return sinkOp.value();
                        }(plan);

                        sinkOperator.sinkDescriptor = std::make_shared<Sinks::SinkDescriptor>(sinkExpected.value());
                        plan.rootOperators.at(0) = sinkOperator;
                        currentTest.setQuery(std::move(query), std::move(plan));
                    }
                    catch (Exception& e)
                    {
                        currentTest.setQuery(std::move(query), std::unexpected{e});
                    }
                }
            });

        parser.registerOnErrorExpectationCallback(
            [&](const SystestParser::ErrorExpectation& errorExpectation, const SystestQueryId correspondingQueryId)
            {
                /// Error always belongs to the last parsed plan
                plans.emplace(correspondingQueryId, correspondingQueryId)
                    .first->second.setExpectedResult(ExpectedError{.code = errorExpectation.code, .message = errorExpectation.message});
            });

        parser.registerOnResultTuplesCallback(
            [&](std::vector<std::string>&& resultTuples, const SystestQueryId correspondingQueryId)
            { plans.emplace(correspondingQueryId, correspondingQueryId).first->second.setExpectedResult(std::move(resultTuples)); });
        try
        {
            parser.parse();
        }
        catch (Exception& exception)
        {
            tryLogCurrentException();
            exception.what() += fmt::format("Could not successfully parse test file://{}", testFilePath.string());
            throw;
        }
        return plans
            | std::ranges::views::transform(
                   [&testFilePath, this, testFileName, &sourceThreads](auto& pair)
                   {
                       pair.second.setPaths(testFilePath, workingDir);
                       pair.second.setName(std::string{testFileName});
                       pair.second.setAdditionalSourceThreads(sourceThreads);
                       return pair.second;
                   })
            | std::ranges::to<std::vector>();
    }
    /// NOLINTEND(readability-function-cognitive-complexity)

private:
    std::filesystem::path workingDir;
    std::filesystem::path testDataDir;
    std::filesystem::path configDir;
};


SystestBinder::SystestBinder(
    const std::filesystem::path& workingDir, const std::filesystem::path& testDataDir, const std::filesystem::path& configDir)
    : impl(std::make_unique<Impl>(workingDir, testDataDir, configDir))
{
}

std::vector<SystestQuery> SystestBinder::loadOptimizeQueries(const TestFileMap& discoveredTestFiles)
{
    return impl->loadOptimizeQueries(discoveredTestFiles);
}
SystestBinder::~SystestBinder() = default;

}
