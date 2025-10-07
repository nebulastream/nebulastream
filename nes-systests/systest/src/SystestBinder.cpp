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
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <regex>
#include <sstream>
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
#include <LegacyOptimizer/LegacyOptimizer.hpp>
#include <LegacyOptimizer/QueryPlanning.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDataProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <SystestState.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES::Systest
{

/// Helper class to model the two-step process of creating sinks in systest. We cannot create sink descriptors directly from sink definitions, because
/// every query should write to a separate file sink, while being able to share the sink definitions with other queries.
class SLTSinkFactory
{
public:
    explicit SLTSinkFactory(std::shared_ptr<SinkCatalog> sinkCatalog) : sinkCatalog(std::move(sinkCatalog)) { }

    bool registerSink(const std::string& sinkType, const std::string_view sinkNameInFile, const Schema& schema)
    {
        auto [_, success] = sinkProviders.emplace(
            sinkNameInFile,
            [this, schema, sinkType](
                const std::string_view assignedSinkName, std::filesystem::path filePath) -> std::expected<SinkDescriptor, Exception>
            {
                std::unordered_map<std::string, std::string> config{{"file_path", std::move(filePath)}};
                if (sinkType == "File")
                {
                    config["input_format"] = "CSV";
                }
                const auto sink
                    = sinkCatalog->addSinkDescriptor(std::string{assignedSinkName}, schema, sinkType, "localhost:9090", std::move(config));
                if (not sink.has_value())
                {
                    return std::unexpected{SinkAlreadyExists("Failed to create file sink with assigned name {}", assignedSinkName)};
                }
                return sink.value();
            });
        return success;
    }

    std::expected<SinkDescriptor, Exception>
    createActualSink(const std::string& sinkNameInFile, const std::string_view assignedSinkName, const std::filesystem::path& filePath)
    {
        const auto sinkProviderIter = sinkProviders.find(sinkNameInFile);
        if (sinkProviderIter == sinkProviders.end())
        {
            throw UnknownSinkName("{}", sinkNameInFile);
        }
        return sinkProviderIter->second(std::string{assignedSinkName}, filePath);
    }

    static inline const Schema checksumSchema = []
    {
        auto checksumSinkSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
        checksumSinkSchema.addField("S$Count", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        checksumSinkSchema.addField("S$Checksum", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        return checksumSinkSchema;
    }();

private:
    SharedPtr<SinkCatalog> sinkCatalog;
    std::unordered_map<std::string, std::function<std::expected<SinkDescriptor, Exception>(std::string_view, std::filesystem::path)>>
        sinkProviders;
};

/// A Builder for Systest queries that matches the steps in which information is added.
/// Contains logic to extract some more information from the set fields, and to validate that all fields have been set.
class SystestQueryBuilder
{
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

    void setQueryDefinition(std::string queryDefinition) { this->queryDefinition = std::move(queryDefinition); }

    void setBoundPlan(LogicalPlan boundPlan) { this->boundPlan = std::move(boundPlan); }

    void setException(const Exception& exception) { this->exception = exception; }

    std::expected<LogicalPlan, Exception> getBoundPlan() const
    {
        if (boundPlan.has_value())
        {
            return boundPlan.value();
        }
        return std::unexpected{TestException("No bound plan set")};
    }

    void setOptimizedPlan(PlanStage::DistributedLogicalPlan optimizedPlan)
    {
        this->optimizedPlan = std::move(optimizedPlan);
        std::unordered_map<SourceDescriptor, std::pair<SourceInputFile, uint64_t>> sourceNamesToFilepathAndCountForQuery;
        std::ranges::for_each(
            getOperatorByType<SourceDescriptorLogicalOperator>(*this->optimizedPlan),
            [&sourceNamesToFilepathAndCountForQuery](const auto& logicalSourceOperator)
            {
                if (const auto path
                    = logicalSourceOperator->getSourceDescriptor().template tryGetFromConfig<std::string>(std::string{"file_path"});
                    path.has_value())
                {
                    if (auto entry = sourceNamesToFilepathAndCountForQuery.extract(logicalSourceOperator->getSourceDescriptor());
                        entry.empty())
                    {
                        sourceNamesToFilepathAndCountForQuery.emplace(
                            logicalSourceOperator->getSourceDescriptor(), std::make_pair(SourceInputFile{*path}, 1));
                    }
                    else
                    {
                        entry.mapped().second++;
                        sourceNamesToFilepathAndCountForQuery.insert(std::move(entry));
                    }
                }
                else
                {
                    NES_INFO(
                        "No file found for physical source {} for logical source {}",
                        logicalSourceOperator->getSourceDescriptor().getPhysicalSourceId(),
                        logicalSourceOperator->getSourceDescriptor().getLogicalSource().getLogicalSourceName());
                }
            });
        this->sourcesToFilePathsAndCounts = std::move(sourceNamesToFilepathAndCountForQuery);
        const auto sinkOperatorOpt = this->optimizedPlan->getGlobalPlan().getRootOperators().at(0).tryGetAs<SinkLogicalOperator>();
        INVARIANT(sinkOperatorOpt.has_value(), "The optimized plan should have a sink operator");
        INVARIANT(sinkOperatorOpt.value()->getSinkDescriptor().has_value(), "The sink operator should have a sink descriptor");
        if (sinkOperatorOpt.value()->getSinkDescriptor().value().getSinkType() == "Checksum") /// NOLINT(bugprone-unchecked-optional-access)
        {
            sinkOutputSchema = SLTSinkFactory::checksumSchema;
        }
        else
        {
            sinkOutputSchema = this->optimizedPlan->getGlobalPlan().getRootOperators().at(0).getOutputSchema();
        }
    }

    void setDifferentialQueryPlan(LogicalPlan differentialQueryPlan) { this->differentialQueryPlan = std::move(differentialQueryPlan); }

    void setOptimizedDifferentialQueryPlan(PlanStage::DistributedLogicalPlan differentialQueryPlan)
    {
        this->optimizedDifferentialQueryPlan = std::move(differentialQueryPlan);
    }

    void optimizeQueries(SharedPtr<SourceCatalog> sourceCatalog, SharedPtr<SinkCatalog> sinkCatalog, SharedPtr<WorkerCatalog> workerCatalog)
    {
        if (!boundPlan.has_value())
        {
            return;
        }
        try
        {
            QueryPlanningContext planningContext{
                .sqlString = boundPlan->getOriginalSql(),
                .sourceCatalog = Util::copyPtr(sourceCatalog),
                .sinkCatalog = Util::copyPtr(sinkCatalog),
                .workerCatalog = Util::copyPtr(workerCatalog)};
            setOptimizedPlan(QueryPlanner::with(std::move(planningContext)).plan(PlanStage::BoundLogicalPlan{*std::move(boundPlan)}));
        }
        catch (Exception& e)
        {
            setException(e);
            return;
        }

        /// Optimize differential query if it exists
        if (differentialQueryPlan.has_value())
        {
            try
            {
                QueryPlanningContext planningContext{
                    .sqlString = differentialQueryPlan->getOriginalSql(),
                    .sourceCatalog = Util::copyPtr(sourceCatalog),
                    .sinkCatalog = Util::copyPtr(sinkCatalog),
                    .workerCatalog = Util::copyPtr(workerCatalog)};
                setOptimizedDifferentialQueryPlan(
                    QueryPlanner::with(std::move(planningContext)).plan(PlanStage::BoundLogicalPlan{std::move(*differentialQueryPlan)}));
            }
            catch (Exception& e)
            {
                setException(e);
            }
        }
    }

    /// NOLINTBEGIN(bugprone-unchecked-optional-access)
    SystestQuery build() &&
    {
        PRECONDITION(not built, "Cannot build a SystestQuery twice");
        built = true;
        PRECONDITION(testName.has_value(), "Test name has not been set");
        PRECONDITION(testFilePath.has_value(), "Test file path has not been set");
        PRECONDITION(workingDir.has_value(), "Working directory has not been set");
        PRECONDITION(queryDefinition.has_value(), "Query definition has not been set");
        if (not exception.has_value())
        {
            PRECONDITION(
                expectedResultsOrError.has_value() || differentialQueryPlan.has_value(),
                "Differential query plan or error has not been set");
        }
        const auto createPlanInfoOrException = [this]() -> std::expected<SystestQuery::PlanInfo, Exception>
        {
            if (not exception.has_value())
            {
                PRECONDITION(
                    boundPlan.has_value() && optimizedPlan.has_value() && sourcesToFilePathsAndCounts.has_value()
                        && sinkOutputSchema.has_value() && additionalSourceThreads.has_value(),
                    "Neither optimized plan nor an exception has been set");
                return SystestQuery::PlanInfo{
                    .queryPlan = std::move(optimizedPlan.value()),
                    .sourcesToFilePathsAndCounts = std::move(sourcesToFilePathsAndCounts.value()),
                    .sinkOutputSchema = sinkOutputSchema.value(),
                };
            }
            return std::unexpected{exception.value()};
        };
        auto expectedResultsValue = expectedResultsOrError.has_value()
            ? std::move(expectedResultsOrError.value())
            : std::variant<std::vector<std::string>, ExpectedError>{std::vector<std::string>{}};

        return SystestQuery{
            .testName = std::move(testName.value()),
            .queryIdInFile = queryIdInFile,
            .testFilePath = std::move(testFilePath.value()),
            .workingDir = std::move(workingDir.value()),
            .queryDefinition = std::move(queryDefinition.value()),
            .planInfoOrException = createPlanInfoOrException(),
            .expectedResultsOrExpectedError = std::move(expectedResultsValue),
            .additionalSourceThreads = std::move(additionalSourceThreads.value()),
            .differentialQueryPlan = std::move(optimizedDifferentialQueryPlan)};
    }

    /// NOLINTEND(bugprone-unchecked-optional-access)

private:
    /// We could make all the fields just public and set them, but since some setters contain more complex logic, I wanted to keep access uniform.
    std::optional<TestName> testName;
    SystestQueryId queryIdInFile;
    std::optional<std::filesystem::path> testFilePath;
    std::optional<std::filesystem::path> workingDir;
    std::optional<std::string> queryDefinition;
    std::optional<LogicalPlan> boundPlan;
    std::optional<Exception> exception;
    std::optional<PlanStage::DistributedLogicalPlan> optimizedPlan;
    std::optional<std::unordered_map<SourceDescriptor, std::pair<SourceInputFile, uint64_t>>> sourcesToFilePathsAndCounts;
    std::optional<Schema> sinkOutputSchema;
    std::optional<std::variant<std::vector<std::string>, ExpectedError>> expectedResultsOrError;
    std::optional<std::shared_ptr<std::vector<std::jthread>>> additionalSourceThreads;
    std::optional<LogicalPlan> differentialQueryPlan;
    std::optional<PlanStage::DistributedLogicalPlan> optimizedDifferentialQueryPlan;
    bool built = false;
};

struct SystestBinder::Impl
{
    explicit Impl(std::filesystem::path workingDir, std::filesystem::path testDataDir, std::filesystem::path configDir)
        : workingDir(std::move(workingDir)), testDataDir(std::move(testDataDir)), configDir(std::move(configDir))
    {
        this->workerCatalog = std::make_shared<WorkerCatalog>();
        workerCatalog->addWorker(HostAddr("localhost:9090"), GrpcAddr("localhost:8080"), INFINITE_CAPACITY, {});
    }

    std::pair<std::vector<SystestQuery>, size_t> loadOptimizeQueries(const TestFileMap& discoveredTestFiles)
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
        std::cout << fmt::format(
            "Loaded {}/{} test files containing a total of {} queries\n", loadedFiles, discoveredTestFiles.size(), queries.size())
                  << std::flush;
        return std::make_pair(queries, loadedFiles);
    }

    std::vector<SystestQuery> loadOptimizeQueriesFromTestFile(const Systest::TestFile& testfile)
    {
        SLTSinkFactory sinkProvider{testfile.sinkCatalog};
        auto loadedSystests = loadFromSLTFile(testfile.file, testfile.name(), testfile.sourceCatalog, sinkProvider);
        std::unordered_set<SystestQueryId> foundQueries;

        auto buildSystests = loadedSystests
            | std::views::filter(
                                 [&testfile](const auto& loadedQueryPlan)
                                 {
                                     return testfile.onlyEnableQueriesWithTestQueryNumber.empty()
                                         or testfile.onlyEnableQueriesWithTestQueryNumber.contains(loadedQueryPlan.getSystemTestQueryId());
                                 })
            | std::ranges::views::transform(
                                 [&testfile, &foundQueries, this](auto& systest)
                                 {
                                     foundQueries.insert(systest.getSystemTestQueryId());
                                     systest.optimizeQueries(testfile.sourceCatalog, testfile.sinkCatalog, Util::copyPtr(workerCatalog));
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

    static void createLogicalSource(const std::shared_ptr<SourceCatalog>& sourceCatalog, const CreateLogicalSourceStatement& statement)
    {
        const auto created = sourceCatalog->addLogicalSource(statement.name, statement.schema);
        if (not created.has_value())
        {
            throw InvalidQuerySyntax();
        }
    }

    [[nodiscard]] static std::filesystem::path generateSourceFilePath() { return std::tmpnam(nullptr); }

    [[nodiscard]] std::filesystem::path generateSourceFilePath(const std::string& testData) const { return testDataDir / testData; }

    [[nodiscard]] PhysicalSourceConfig setUpSourceWithTestData(
        PhysicalSourceConfig& physicalSourceConfig,
        std::shared_ptr<std::vector<std::jthread>> sourceThreads,
        std::pair<TestDataIngestionType, std::vector<std::string>> testData) const
    {
        switch (testData.first)
        {
            case TestDataIngestionType::INLINE: {
                const auto testFile = generateSourceFilePath();
                return SourceDataProvider::provideInlineDataSource(
                    std::move(physicalSourceConfig), std::move(testData.second), std::move(sourceThreads), testFile);
            }
            case TestDataIngestionType::FILE: {
                if (testData.second.size() != 1)
                {
                    throw UnknownException("Invalid State");
                }

                const std::filesystem::path testFilePath = generateSourceFilePath(testData.second[0]);
                return SourceDataProvider::provideFileDataSource(std::move(physicalSourceConfig), std::move(sourceThreads), testFilePath);
            }
            default:
                std::unreachable();
        }
    }

    void createPhysicalSource(
        const std::shared_ptr<SourceCatalog>& sourceCatalog,
        const std::shared_ptr<std::vector<std::jthread>>& sourceThreads,
        const CreatePhysicalSourceStatement& statement,
        std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> testData) const
    {
        PhysicalSourceConfig physicalSourceConfig{
            .logical = statement.attachedTo.getRawValue(),
            .type = statement.sourceType,
            .parserConfig = statement.parserConfig,
            .sourceConfig = statement.sourceConfig};

        std::unordered_map<std::string, std::string> defaultParserConfig{{"type", "CSV"}};
        physicalSourceConfig.parserConfig.merge(defaultParserConfig);

        if (testData.has_value())
        {
            physicalSourceConfig = setUpSourceWithTestData(physicalSourceConfig, sourceThreads, std::move(testData.value()));
        }

        const auto logicalSource = sourceCatalog->getLogicalSource(statement.attachedTo.getRawValue());
        if (not logicalSource.has_value())
        {
            throw UnknownSourceName("{}", statement.attachedTo.getRawValue());
        }

        if (const auto created = sourceCatalog->addPhysicalSource(
                *logicalSource,
                physicalSourceConfig.type,
                "localhost:9090",
                physicalSourceConfig.sourceConfig,
                physicalSourceConfig.parserConfig))
        {
            return;
        }

        throw InvalidQuerySyntax();
    }

    static void createSink(SLTSinkFactory& sltSinkProvider, const CreateSinkStatement& statement)
    {
        Schema schema{Schema::MemoryLayoutType::ROW_LAYOUT};
        for (const auto& field : statement.schema.getFields())
        {
            schema.addField(field.name, field.dataType);
        }
        sltSinkProvider.registerSink(statement.sinkType, statement.name, schema);
    }

    void createCallback(
        const StatementBinder& binder,
        const std::shared_ptr<SourceCatalog>& sourceCatalog,
        SLTSinkFactory& sltSinkProvider,
        const std::shared_ptr<std::vector<std::jthread>>& sourceThreads,
        const std::string& query,
        std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> testData) const
    {
        const auto managedParser = NES::AntlrSQLQueryParser::ManagedAntlrParser::create(query);
        const auto parseResult = managedParser->parseSingle();
        if (not parseResult.has_value())
        {
            throw InvalidQuerySyntax("failed to to parse the query \"{}\"", Util::replaceAll(query, "\n", " "));
        }

        const auto binding = binder.bind(parseResult.value().get());
        if (not binding.has_value())
        {
            throw InvalidQuerySyntax("failed to to parse the query \"{}\"", Util::replaceAll(query, "\n", " "));
        }

        if (const auto& statement = binding.value(); std::holds_alternative<CreateLogicalSourceStatement>(statement))
        {
            createLogicalSource(sourceCatalog, std::get<CreateLogicalSourceStatement>(statement));
        }
        else if (std::holds_alternative<CreatePhysicalSourceStatement>(statement))
        {
            createPhysicalSource(sourceCatalog, sourceThreads, std::get<CreatePhysicalSourceStatement>(statement), std::move(testData));
        }
        else if (std::holds_alternative<CreateSinkStatement>(statement))
        {
            createSink(sltSinkProvider, std::get<CreateSinkStatement>(statement));
        }
        else
        {
            throw UnsupportedQuery();
        }
    }

    void queryCallback(
        const std::string_view& testFileName,
        std::unordered_map<SystestQueryId, SystestQueryBuilder>& plans,
        SLTSinkFactory& sltSinkProvider,
        std::string query,
        const SystestQueryId& currentQueryNumberInTest)
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
        const auto sinkForQuery = Util::toUpperCase(sinkName + std::to_string(currentQueryNumberInTest.getRawValue()));
        query = std::regex_replace(query, std::regex(sinkName), sinkForQuery);

        /// Adding the sink to the sink config, such that we can create a fully specified query plan
        const auto resultFile = SystestQuery::resultFile(workingDir, testFileName, currentQueryNumberInTest);

        SystestQueryBuilder currentBuilder{currentQueryNumberInTest};
        currentBuilder.setQueryDefinition(query);
        if (auto sinkExpected = sltSinkProvider.createActualSink(Util::toUpperCase(sinkName), sinkForQuery, resultFile);
            not sinkExpected.has_value())
        {
            currentBuilder.setException(sinkExpected.error());
        }
        else
        {
            try
            {
                auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
                currentBuilder.setBoundPlan(std::move(plan));
            }
            catch (Exception& e)
            {
                currentBuilder.setException(e);
            }
        }
        plans.emplace(currentQueryNumberInTest, currentBuilder);
    }

    static void errorExpectationCallback(
        std::unordered_map<SystestQueryId, SystestQueryBuilder>& plans,
        SystestParser::ErrorExpectation errorExpectation,
        SystestQueryId correspondingQueryId)
    {
        /// Error always belongs to the last parsed plan
        plans.emplace(correspondingQueryId, correspondingQueryId)
            .first->second.setExpectedResult(
                ExpectedError{.code = std::move(errorExpectation.code), .message = std::move(errorExpectation.message)});
    }

    static void resultTuplesCallback(
        std::unordered_map<SystestQueryId, SystestQueryBuilder>& plans,
        std::vector<std::string>&& resultTuples,
        const SystestQueryId& correspondingQueryId)
    {
        plans.emplace(correspondingQueryId, correspondingQueryId).first->second.setExpectedResult(std::move(resultTuples));
    }

    void differentialQueryBlocksCallback(
        SystestQueryId& lastParsedQueryId,
        const std::string_view& testFileName,
        std::unordered_map<SystestQueryId, SystestQueryBuilder>& plans,
        SLTSinkFactory& sltSinkProvider,
        std::string leftQuery,
        std::string rightQuery,
        const SystestQueryId currentQueryNumberInTest) const
    {
        const auto extractSinkName = [](const std::string& query) -> std::string
        {
            std::istringstream stream(query);
            std::string token;
            while (stream >> token)
            {
                if (Util::toLowerCase(token) == "into")
                {
                    std::string sink;
                    if (!(stream >> sink))
                    {
                        NES_ERROR("INTO clause not followed by sink name in query: {}", query);
                        return "";
                    }
                    if (!sink.empty() && sink.back() == ';')
                    {
                        sink.pop_back();
                    }
                    return sink;
                }
            }
            NES_ERROR("INTO clause not found in query: {}", query);
            return "";
        };

        const auto leftSinkName = extractSinkName(leftQuery);
        const auto rightSinkName = extractSinkName(rightQuery);

        const auto leftSinkForQuery = leftSinkName + std::to_string(lastParsedQueryId.getRawValue());
        const auto rightSinkForQuery = rightSinkName + std::to_string(lastParsedQueryId.getRawValue()) + "differential";

        leftQuery = std::regex_replace(leftQuery, std::regex(leftSinkName), leftSinkForQuery);
        rightQuery = std::regex_replace(rightQuery, std::regex(rightSinkName), rightSinkForQuery);

        const auto differentialTestResultFileName = std::string(testFileName) + "differential";
        const auto leftResultFile = SystestQuery::resultFile(workingDir, testFileName, lastParsedQueryId);
        const auto rightResultFile = SystestQuery::resultFile(workingDir, differentialTestResultFileName, lastParsedQueryId);

        auto& currentTest = plans.emplace(currentQueryNumberInTest, SystestQueryBuilder{currentQueryNumberInTest}).first->second;

        if (auto leftSinkExpected
            = sltSinkProvider.createActualSink(Util::toUpperCase(leftSinkName), Util::toUpperCase(leftSinkForQuery), leftResultFile);
            not leftSinkExpected.has_value())
        {
            currentTest.setException(leftSinkExpected.error());
            return;
        }
        if (auto rightSinkExpected
            = sltSinkProvider.createActualSink(Util::toUpperCase(rightSinkName), Util::toUpperCase(rightSinkForQuery), rightResultFile);
            not rightSinkExpected.has_value())
        {
            currentTest.setException(rightSinkExpected.error());
            return;
        }

        try
        {
            auto leftPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(leftQuery);
            auto rightPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(rightQuery);

            currentTest.setQueryDefinition(std::move(leftQuery));
            currentTest.setBoundPlan(std::move(leftPlan));
            currentTest.setDifferentialQueryPlan(std::move(rightPlan));
        }
        catch (Exception& e)
        {
            currentTest.setException(e);
        }
    }

    std::vector<SystestQueryBuilder> loadFromSLTFile(
        const std::filesystem::path& testFilePath,
        const std::string_view testFileName,
        const std::shared_ptr<NES::SourceCatalog>& sourceCatalog,
        SLTSinkFactory& sltSinkProvider)
    {
        uint64_t sourceIndex = 0;
        /// The bound test cases from this file.
        /// While SystestQueryID is just an integer, unordered maps provide a much easier and safer interface then vectors.
        std::unordered_map<SystestQueryId, SystestQueryBuilder> plans{};
        std::shared_ptr<std::vector<std::jthread>> sourceThreads = std::make_shared<std::vector<std::jthread>>();
        const std::unordered_map<SourceDescriptor, std::filesystem::path> generatedDataPaths{};
        SystestParser parser{};
        const auto binder = NES::StatementBinder{
            sourceCatalog, [](auto&& pH1) { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(pH1)>(pH1)); }};
        if (!parser.loadFile(testFilePath))
        {
            throw TestException("Could not successfully load test file://{}", testFilePath.string());
        }

        /// We create a new query plan from our config when finding a query
        SystestQueryId lastParsedQueryId = INVALID_SYSTEST_QUERY_ID;
        parser.registerOnQueryCallback(
            [&](std::string query, SystestQueryId currentQueryNumberInTest)
            {
                lastParsedQueryId = currentQueryNumberInTest;
                queryCallback(testFileName, plans, sltSinkProvider, std::move(query), std::move(currentQueryNumberInTest));
            });

        parser.registerOnErrorExpectationCallback(
            [&](SystestParser::ErrorExpectation errorExpectation, SystestQueryId correspondingQueryId)
            { errorExpectationCallback(plans, std::move(errorExpectation), std::move(correspondingQueryId)); });

        parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, SystestQueryId correspondingQueryId)
                                              { resultTuplesCallback(plans, std::move(resultTuples), std::move(correspondingQueryId)); });
        parser.registerOnDifferentialQueryBlockCallback(
            [&](std::string leftQuery, std::string rightQuery, SystestQueryId currentQueryNumberInTest, SystestQueryId)
            {
                differentialQueryBlocksCallback(
                    lastParsedQueryId,
                    testFileName,
                    plans,
                    sltSinkProvider,
                    std::move(leftQuery),
                    std::move(rightQuery),
                    std::move(currentQueryNumberInTest));
            });

        parser.registerOnCreateCallback(
            [&, sourceCatalog](const std::string& query, std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> input)
            { createCallback(binder, sourceCatalog, sltSinkProvider, sourceThreads, query, std::move(input)); });

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

private:
    std::filesystem::path workingDir;
    std::filesystem::path testDataDir;
    std::filesystem::path configDir;

    SharedPtr<WorkerCatalog> workerCatalog;
};

SystestBinder::SystestBinder(
    const std::filesystem::path& workingDir, const std::filesystem::path& testDataDir, const std::filesystem::path& configDir)
    : impl(std::make_unique<Impl>(workingDir, testDataDir, configDir))
{
}

std::pair<std::vector<SystestQuery>, size_t> SystestBinder::loadOptimizeQueries(const TestFileMap& discoveredTestFiles)
{
    return impl->loadOptimizeQueries(discoveredTestFiles);
}

SystestBinder::~SystestBinder() = default;

}
