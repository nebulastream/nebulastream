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

#include <Config/Config.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Discovery/TestFileReader.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Model/Expectation.hpp>
#include <Model/TestFile.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/AnonymousSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileParser.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDataProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Files.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Pointers.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <QueryId.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SystestState.hpp>
#include <WorkerCatalog.hpp>

namespace NES::Systest
{

/// Helper class to model the two-step process of creating sinks in systest. We cannot create sink descriptors directly from sink definitions, because
/// every query should write to a separate file sink, while being able to share the sink definitions with other queries.
class SLTSinkFactory
{
public:
    explicit SLTSinkFactory(std::shared_ptr<SinkCatalog> sinkCatalog, std::vector<Host> possibleSinkPlacements)
        : sinkCatalog(std::move(sinkCatalog)), possibleSinkPlacements(std::move(possibleSinkPlacements))
    {
    }

    bool registerSink(
        const Identifier& sinkType,
        const Identifier& sinkNameInFile,
        const Schema<UnqualifiedUnboundField, Ordered>& schema,
        const std::unordered_map<Identifier, std::string>& /*config*/)
    {
        std::unordered_map<Identifier, std::string> config{};
        std::unordered_map<Identifier, std::string> formatConfig{};
        if (sinkType == Identifier::parse("File"))
        {
            config[Identifier::parse("file_path")] = "/tmp/none.txt";
            config[Identifier::parse("output_format")] = "CSV";
        }
        else if (sinkType == Identifier::parse("CHECKSUM"))
        {
            config[Identifier::parse("file_path")] = "/tmp/none.txt";
            formatConfig[Identifier::parse("quote_strings")] = "true";
        }
        std::string host = possibleSinkPlacements.at(0).getRawValue();
        if (auto hostIt = config.find(Identifier::parse("host")); hostIt != config.end())
        {
            host = hostIt->second;
        }

        const auto sink = sinkCatalog->addSinkDescriptor(sinkNameInFile, schema, sinkType, Host(host), std::move(config), formatConfig);
        if (not sink.has_value())
        {
            throw SinkAlreadyExists("Failed to create file sink with assigned name {}", sinkNameInFile);
        }


        auto [_, success] = sinkProviders.emplace(
            sinkNameInFile,
            [this, schema, sinkType](
                Identifier assignedSinkName, const std::filesystem::path& filePath) -> std::expected<SinkDescriptor, Exception>
            {
                /// Only inject a file_path for sink types that consume it. Sinks like Void accept no
                /// configuration parameters, so injecting file_path unconditionally would fail validation.
                std::unordered_map<Identifier, std::string> config{};
                std::unordered_map<Identifier, std::string> formatConfig{};
                if (sinkType == Identifier::parse("File"))
                {
                    config[Identifier::parse("file_path")] = std::move(filePath);
                    config[Identifier::parse("output_format")] = "CSV";
                }
                else if (sinkType == Identifier::parse("CHECKSUM"))
                {
                    config[Identifier::parse("file_path")] = std::move(filePath);
                    formatConfig[Identifier::parse("quote_strings")] = "true";
                }

                PRECONDITION(
                    not possibleSinkPlacements.empty(),
                    "Topology must list at least one worker in allow_sink_placement to assign a default sink host");
                std::string host = possibleSinkPlacements.at(0).getRawValue();
                if (auto hostIt = config.find(Identifier::parse("host")); hostIt != config.end())
                {
                    host = hostIt->second;
                }

                const auto sink
                    = sinkCatalog->addSinkDescriptor(assignedSinkName, schema, sinkType, Host(host), std::move(config), formatConfig);
                if (not sink.has_value())
                {
                    return std::unexpected{SinkAlreadyExists("Failed to create file sink with assigned name {}", assignedSinkName)};
                }
                return sink.value();
            });
        return success;
    }

    std::optional<SinkDescriptor> getAnonymousSink(
        const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
        const Identifier& sinkType,
        std::unordered_map<Identifier, std::string> config,
        const std::unordered_map<Identifier, std::string>& formatConfig)
    {
        PRECONDITION(
            not possibleSinkPlacements.empty(),
            "Topology must list at least one worker in allow_sink_placement to assign a default anonymous sink host");
        return sinkCatalog->getAnonymousSink(
            schema, std::move(sinkType), Host(possibleSinkPlacements.at(0).getRawValue()), std::move(config), formatConfig);
    }

    std::expected<SinkDescriptor, Exception>
    createActualSink(const Identifier& sinkNameInFile, Identifier assignedSinkName, const std::filesystem::path& filePath)
    {
        const auto sinkProviderIter = sinkProviders.find(sinkNameInFile);
        if (sinkProviderIter == sinkProviders.end())
        {
            throw UnknownSinkName("{}", sinkNameInFile);
        }
        return sinkProviderIter->second(std::move(assignedSinkName), filePath);
    }

    /// Function-local static instead of a class-level static: the schema resolves data types
    /// through the DataTypeRegistry, which is populated by loadBuiltinPlugins() at startup —
    /// after static initialization.
    static const Schema<UnqualifiedUnboundField, Ordered>& checksumSchema()
    {
        static const Schema<UnqualifiedUnboundField, Ordered> ChecksumSchema{std::vector{
            UnqualifiedUnboundField{Identifier::parse("COUNT"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            UnqualifiedUnboundField{Identifier::parse("CHECKSUM"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}}};
        return ChecksumSchema;
    }

private:
    SharedPtr<SinkCatalog> sinkCatalog;
    std::vector<Host> possibleSinkPlacements;
    std::unordered_map<Identifier, std::function<std::expected<SinkDescriptor, Exception>(Identifier, std::filesystem::path)>>
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

    void setExpectation(Expectation expectation) { this->expectation = std::move(expectation); }

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

    void setConfigurationOverrides(std::vector<ConfigurationOverride> overrides) { configurationOverrides = std::move(overrides); }

    void setQueryDefinition(std::string queryDefinition) { this->queryDefinition = std::move(queryDefinition); }

    void setBoundPlan(LogicalPlan boundPlan) { this->boundPlan = std::move(boundPlan); }

    void setException(const Exception& exception) { this->exception = exception; }

    void setRunAfter(std::pair<TestName, SystestQueryId> runAfter) { this->runAfter = runAfter; }

    std::expected<LogicalPlan, Exception> getBoundPlan() const
    {
        if (boundPlan.has_value())
        {
            return boundPlan.value();
        }
        return std::unexpected{TestException("No bound plan set")};
    }

    void setOptimizedPlan(DistributedLogicalPlan optimizedPlan)
    {
        this->optimizedPlan = std::move(optimizedPlan);
        std::unordered_map<SourceDescriptor, std::pair<SourceInputFile, uint64_t>> sourceNamesToFilepathAndCountForQuery;
        std::ranges::for_each(
            getOperatorByType<SourceDescriptorLogicalOperator>(this->optimizedPlan->getGlobalPlan()),
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
        this->sourcesToFilePathsAndCounts.emplace(std::move(sourceNamesToFilepathAndCountForQuery));
        const auto sinkOperatorOpt = this->optimizedPlan->getGlobalPlan().getRootOperators().at(0).tryGetAs<SinkLogicalOperator>();
        INVARIANT(sinkOperatorOpt.has_value(), "The optimized plan should have a sink operator");
        INVARIANT(sinkOperatorOpt.value()->getSinkDescriptor().has_value(), "The sink operator should have a sink descriptor");
        if (toUpperCase(sinkOperatorOpt.value()->getSinkDescriptor().value().getSinkType()) /// NOLINT(bugprone-unchecked-optional-access)
            == "CHECKSUM")
        {
            sinkOutputSchema = SLTSinkFactory::checksumSchema();
        }
        else
        {
            sinkOutputSchema = [&]
            {
                /// Sinks do not have an output schema, but they are guaranteed to have only one child, from which we can take the output schema
                const auto sink = this->optimizedPlan->getGlobalPlan().getRootOperators().at(0).tryGetAs<SinkLogicalOperator>();
                if (!sink.has_value())
                {
                    throw InvalidQuerySyntax("The optimized plan should have a sink as its root");
                }
                return *get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(
                    sink.value()->getSinkDescriptor()->getSchema());
            }();
        }
    }

    void setDifferentialQueryPlan(LogicalPlan differentialQueryPlan) { this->differentialQueryPlan = std::move(differentialQueryPlan); }

    void setExplainStatement(ExplainQueryStatement statement) { this->explainStatement = std::move(statement); }

    void optimizeQueries(const NES::QueryOptimizer& queryOptimizer)
    {
        if (explainStatement.has_value())
        {
            /// EXPLAIN statements are never executed; compute the explain output now, as this is the only place with
            /// access to the query optimizer (needed for the OPTIMIZED, DISTRIBUTED and ALL stages).
            try
            {
                actualExplainOutput = computeExplainOutput(explainStatement.value(), queryOptimizer);
            }
            catch (Exception& e)
            {
                setException(e);
            }
            return;
        }
        if (!boundPlan.has_value())
        {
            return;
        }
        try
        {
            auto distributedPlan = queryOptimizer.optimize(boundPlan.value());
            setOptimizedPlan(std::move(distributedPlan));
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
                auto distributedPlan = queryOptimizer.optimize(differentialQueryPlan.value());
                this->optimizedDifferentialQueryPlan = std::move(distributedPlan);
            }
            catch (Exception& e)
            {
                setException(e);
            }
        }
    }

    /// NOLINTBEGIN(bugprone-unchecked-optional-access)
    std::vector<SystestQuery> build() &&
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
                expectation.has_value() || differentialQueryPlan.has_value(), "Differential query plan or expectation has not been set");
        }

        if (explainStatement.has_value())
        {
            /// EXPLAIN statements have no executable plan and never touch the worker, so configuration overrides do not
            /// apply and exactly one query is emitted. On success, the runner only reads actualExplainOutput.
            return {
                {.testName = testName.value(),
                 .queryIdInFile = queryIdInFile,
                 .testFilePath = testFilePath.value(),
                 .workingDir = workingDir.value(),
                 .queryDefinition = queryDefinition.value(),
                 .planInfoOrException = std::
                     unexpected{exception.has_value() ? exception.value() : Exception{TestException("EXPLAIN statements are not executed and have no plan info")}},
                 .expectation = expectation.value_or(Expectation{ExpectedRows{}}),
                 .additionalSourceThreads = additionalSourceThreads.value(),
                 .configurationOverride = ConfigurationOverride{},
                 .differentialQueryPlan = std::nullopt,
                 .runAfter = runAfter,
                 .actualExplainOutput = exception.has_value() ? std::nullopt : actualExplainOutput}};
        }

        const auto createPlanInfoOrException = [this]() -> std::expected<SystestQuery::PlanInfo, Exception>
        {
            if (not exception.has_value())
            {
                PRECONDITION(
                    boundPlan.has_value() && optimizedPlan.has_value() && sourcesToFilePathsAndCounts.has_value()
                        && sinkOutputSchema.has_value() && additionalSourceThreads.has_value(),
                    "Neither optimized plan nor an exception has been set");
                return SystestQuery::PlanInfo{optimizedPlan.value(), sourcesToFilePathsAndCounts.value(), sinkOutputSchema.value()};
            }
            return std::unexpected{exception.value()};
        };
        const auto expectationValue = expectation.value_or(Expectation{ExpectedRows{}});

        auto planInfoTemplate = createPlanInfoOrException();

        std::vector<SystestQuery> queries;
        queries.reserve(configurationOverrides.size());
        for (const auto& configurationOverride : configurationOverrides)
        {
            queries.push_back(
                {.testName = testName.value(),
                 .queryIdInFile = queryIdInFile,
                 .testFilePath = testFilePath.value(),
                 .workingDir = workingDir.value(),
                 .queryDefinition = queryDefinition.value(),
                 .planInfoOrException = planInfoTemplate,
                 .expectation = expectationValue,
                 .additionalSourceThreads = additionalSourceThreads.value(),
                 .configurationOverride = std::move(configurationOverride),
                 .differentialQueryPlan = optimizedDifferentialQueryPlan,
                 .runAfter = runAfter,
                 .actualExplainOutput = std::nullopt});
        }
        return queries;
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
    std::optional<DistributedLogicalPlan> optimizedPlan;
    std::optional<std::unordered_map<SourceDescriptor, std::pair<SourceInputFile, uint64_t>>> sourcesToFilePathsAndCounts;
    std::optional<Schema<UnqualifiedUnboundField, Ordered>> sinkOutputSchema;
    std::optional<Expectation> expectation;
    std::optional<std::shared_ptr<std::vector<std::jthread>>> additionalSourceThreads;
    std::vector<ConfigurationOverride> configurationOverrides{ConfigurationOverride{}};
    std::optional<LogicalPlan> differentialQueryPlan;
    std::optional<DistributedLogicalPlan> optimizedDifferentialQueryPlan;
    std::optional<std::pair<TestName, SystestQueryId>> runAfter;
    std::optional<ExplainQueryStatement> explainStatement;
    std::optional<std::string> actualExplainOutput;
    bool built = false;
};

struct SystestBinder::Impl
{
    explicit Impl(
        std::filesystem::path workingDir,
        std::filesystem::path testDataDir,
        std::filesystem::path configDir,
        QueryOptimizerConfiguration queryOptimizerConfiguration,
        SystestClusterConfiguration clusterConfiguration)
        : workingDir(std::move(workingDir))
        , testDataDir(std::move(testDataDir))
        , configDir(std::move(configDir))
        , queryOptimizerConfiguration(std::move(queryOptimizerConfiguration))
        , clusterConfiguration(std::move(clusterConfiguration))
    {
        this->workerCatalog = std::make_shared<WorkerCatalog>();
        for (const auto& [host, data, capacity, downstream, config] : this->clusterConfiguration.workers)
        {
            workerCatalog->addWorker(host, data, capacity, downstream, config);
        }
    }

    std::pair<std::vector<SystestQuery>, size_t> loadOptimizeQueries(const DiscoveredTestFiles& discoveredTestFiles)
    {
        /// This method could also be removed with the checks and loop put in the SystestExecutor, but it's an aesthetic choice.
        std::vector<SystestQuery> queries;
        uint64_t loadedFiles = 0;

        for (const auto& testfile : discoveredTestFiles)
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

    std::vector<SystestQuery> loadOptimizeQueriesFromTestFile(const DiscoveredTestFile& testfile)
    {
        /// Each test file names its sources and sinks in its own terms, so each one binds against its own catalogs.
        const auto sourceCatalog = std::make_shared<SourceCatalog>();
        const auto sinkCatalog = std::make_shared<SinkCatalog>();

        SLTSinkFactory sinkProvider{sinkCatalog, clusterConfiguration.allowSinkPlacement};
        auto modelCatalog = std::make_shared<ModelCatalog>();
        auto loadedSystests = loadFromSLTFile(testfile.file, testfile.name(), sourceCatalog, modelCatalog, sinkProvider);
        std::unordered_set<SystestQueryId> foundQueries;

        const QueryOptimizer queryOptimizer{queryOptimizerConfiguration, sourceCatalog, sinkCatalog, copyPtr(workerCatalog), modelCatalog};

        std::vector<SystestQuery> buildSystests;
        for (auto& builder : loadedSystests)
        {
            const bool includeBuilder = testfile.onlyEnableQueriesWithTestQueryNumber.empty()
                || testfile.onlyEnableQueriesWithTestQueryNumber.contains(builder.getSystemTestQueryId());
            if (!includeBuilder)
            {
                continue;
            }

            foundQueries.insert(builder.getSystemTestQueryId());
            builder.optimizeQueries(queryOptimizer);
            for (auto& query : std::move(builder).build())
            {
                buildSystests.emplace_back(std::move(query));
            }
        }

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

    [[nodiscard]] std::filesystem::path generateSourceFilePath() const
    {
        auto sourceDir = workingDir / "sources";
        if (not is_directory(sourceDir))
        {
            create_directory(sourceDir);
            std::cout << "Created sources directory: file://" << sourceDir.string() << "\n";
        }

        return createUniqueFile(fmt::format("{}/input", sourceDir), ".csv").second;
    }

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
        PRECONDITION(
            not clusterConfiguration.allowSourcePlacement.empty(),
            "Topology must list at least one worker in allow_source_placement to assign a default source host");
        const auto host = statement.host ? *statement.host : Host(clusterConfiguration.allowSourcePlacement.at(0).getRawValue());

        PhysicalSourceConfig physicalSourceConfig{
            .logical = statement.attachedTo.asCanonicalString(),
            .type = statement.sourceType,
            .parserConfig = statement.parserConfig,
            .sourceConfig = statement.sourceConfig};

        std::unordered_map<Identifier, std::string> defaultParserConfig{{Identifier::parse("type"), "CSV"}};
        physicalSourceConfig.parserConfig.merge(defaultParserConfig);

        if (testData.has_value())
        {
            physicalSourceConfig = setUpSourceWithTestData(physicalSourceConfig, sourceThreads, std::move(testData.value()));
        }

        const auto logicalSource = sourceCatalog->getLogicalSource(statement.attachedTo);
        if (not logicalSource.has_value())
        {
            throw UnknownSourceName("{}", statement.attachedTo);
        }

        if (const auto created = sourceCatalog->addPhysicalSource(
                *logicalSource, physicalSourceConfig.type, host, physicalSourceConfig.sourceConfig, physicalSourceConfig.parserConfig);
            not created.has_value())
        {
            throw Exception(created.error());
        }
    }

    static void createSink(SLTSinkFactory& sltSinkProvider, const CreateSinkStatement& statement)
    {
        sltSinkProvider.registerSink(statement.sinkType, statement.name, statement.schema, statement.sinkConfig);
    }

    void createModel(const std::shared_ptr<ModelCatalog>& modelCatalog, const CreateModelStatement& statement) const
    {
        /// Resolve relative paths against testDataDir before routing through the handler
        auto resolvedStatement = statement;
        auto path = std::filesystem::path(statement.path);
        if (!path.is_absolute())
        {
            path = testDataDir / path;
        }
        resolvedStatement.path = path.string();

        auto handler = ModelStatementHandler(modelCatalog);
        auto result = handler(resolvedStatement);
        if (!result)
        {
            throw std::move(result).error();
        }
    }

    void bindCreateStatement(
        const StatementBinder& binder,
        const std::shared_ptr<SourceCatalog>& sourceCatalog,
        const std::shared_ptr<ModelCatalog>& modelCatalog,
        SLTSinkFactory& sltSinkProvider,
        const std::shared_ptr<std::vector<std::jthread>>& sourceThreads,
        const std::string& query,
        std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> testData) const
    {
        const auto managedParser = NES::AntlrSQLQueryParser::ManagedAntlrParser::create(query);
        const auto parseResult = managedParser->parseSingle();
        if (not parseResult.has_value())
        {
            throw InvalidQuerySyntax("failed to to parse the query \"{}\"", replaceAll(query, "\n", " "));
        }

        const auto binding = binder.bind(parseResult.value().get());
        if (not binding.has_value())
        {
            throw InvalidQuerySyntax("failed to to parse the query \"{}\"", replaceAll(query, "\n", " "));
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
        else if (std::holds_alternative<CreateModelStatement>(statement))
        {
            createModel(modelCatalog, std::get<CreateModelStatement>(statement));
        }
        else
        {
            throw UnsupportedQuery();
        }
    }

    [[nodiscard]] LogicalOperator updateAnonymousSource(const LogicalOperator& current) const
    {
        std::vector<LogicalOperator> newChildren;
        for (const auto& child : current.getChildren())
        {
            newChildren.emplace_back(updateAnonymousSource(child));
        }

        if (const auto anonymousSource = current.tryGetAs<AnonymousSourceLogicalOperator>())
        {
            auto sourceConfig = anonymousSource.value()->getSourceConfig();
            auto parserConfig = anonymousSource.value()->getParserConfig();

            parserConfig.try_emplace(Identifier::parse("type"), "CSV");

            /// By default, all relative paths are relative to the testDataDir.
            if (sourceConfig.contains(Identifier::parse("file_path")) && !sourceConfig.at(Identifier::parse("file_path")).starts_with("/"))
            {
                auto filePath = anonymousSource.value()->getSourceConfig().at(Identifier::parse("file_path"));
                filePath = testDataDir / filePath;
                sourceConfig.erase(Identifier::parse("file_path"));
                sourceConfig.emplace(Identifier::parse("file_path"), filePath);
            }

            PRECONDITION(
                not clusterConfiguration.allowSourcePlacement.empty(),
                "Topology must list at least one worker in allow_source_placement to assign a default anonymous source host");
            sourceConfig.try_emplace(Identifier::parse("host"), clusterConfiguration.allowSourcePlacement.at(0).getRawValue());

            if (sourceConfig != anonymousSource.value()->getSourceConfig() || parserConfig != anonymousSource.value()->getParserConfig())
            {
                const auto newOperator = AnonymousSourceLogicalOperator::create(
                    anonymousSource.value()->getSourceType(), anonymousSource.value()->getSourceSchema(), sourceConfig, parserConfig);

                return newOperator.withChildrenUnsafe(newChildren);
            }
        }

        return current.withChildrenUnsafe(std::move(newChildren));
    }

    void setAnonymousSources(LogicalPlan& plan) const
    {
        std::vector<LogicalOperator> newRoots;
        for (const auto& root : plan.getRootOperators())
        {
            newRoots.emplace_back(updateAnonymousSource(root));
        }
        plan = plan.withRootOperators(newRoots);
    }

    LogicalOperator setAnonymousSink(
        const std::string_view& testFileName,
        SLTSinkFactory& sltSinkProvider,
        const SystestQueryId& currentQueryNumberInTest,
        const TypedLogicalOperator<AnonymousSinkLogicalOperator>& sinkOperator) const
    {
        const auto resultFile = SystestQuery::resultFile(workingDir, testFileName, currentQueryNumberInTest);

        auto sinkConfig = sinkOperator->getSinkConfig();
        auto formatConfig = sinkOperator->getFormatConfig();
        auto schema = sinkOperator->getTargetSchema();
        sinkConfig.erase(Identifier::parse("file_path"));
        /// Only inject a file_path for a file sink or checksum sink
        if (sinkOperator->getSinkType() == Identifier::parse("File") or sinkOperator->getSinkType() == Identifier::parse("CHECKSUM"))
        {
            sinkConfig.emplace(Identifier::parse("file_path"), resultFile);
        }
        if (not(sinkConfig.contains(Identifier::parse("output_format"))) and sinkOperator->getSinkType() != Identifier::parse("CHECKSUM")
            and sinkOperator->getSinkType() != Identifier::parse("VOID"))
        {
            sinkConfig.emplace(Identifier::parse("output_format"), "CSV");
        }
        if (sinkOperator->getSinkType() == Identifier::parse("CHECKSUM"))
        {
            formatConfig[Identifier::parse("quote_strings")] = "true";
        }

        auto sinkDescriptor = sltSinkProvider.getAnonymousSink(schema, sinkOperator->getSinkType(), sinkConfig, formatConfig);
        if (not sinkDescriptor.has_value())
        {
            throw InvalidConfigParameter("Failed to create anonymous sink of type {}", sinkOperator->getSinkType());
        }
        const auto newOperator = SinkLogicalOperator::create(sinkDescriptor.value());

        return newOperator.withChildrenUnsafe(sinkOperator->getChildren());
    }

    LogicalOperator setNamedSink(
        SystestQueryBuilder& currentBuilder,
        const std::string_view& testFileName,
        SLTSinkFactory& sltSinkProvider,
        const SystestQueryId& currentQueryNumberInTest,
        const TypedLogicalOperator<SinkLogicalOperator>& sinkOperator) const
    {
        const auto sinkNameInFile = sinkOperator->getSinkName();

        /// Replacing the sinkName with the created unique sink name
        const auto sinkForQuery
            = Identifier::parse(toUpperCase(sinkNameInFile.asCanonicalString() + std::to_string(currentQueryNumberInTest.getRawValue())));

        /// Adding the sink to the sink config, such that we can create a fully specified query plan
        const auto resultFile = SystestQuery::resultFile(workingDir, testFileName, currentQueryNumberInTest);

        auto sinkExpected = sltSinkProvider.createActualSink(sinkNameInFile, sinkForQuery, resultFile);
        if (not sinkExpected.has_value())
        {
            currentBuilder.setException(sinkExpected.error());
        }

        const auto newOperator = SinkLogicalOperator::create(sinkExpected.value());

        return newOperator.withChildrenUnsafe(sinkOperator->getChildren());
    }

    void setSinks(
        LogicalPlan& plan,
        SystestQueryBuilder& currentBuilder,
        const std::string_view& testFileName,
        SLTSinkFactory& sltSinkProvider,
        const SystestQueryId& currentQueryNumberInTest) const
    {
        std::vector<LogicalOperator> newRoots;
        for (const auto& rootOperator : plan.getRootOperators())
        {
            if (auto anonymousSink = rootOperator.tryGetAs<AnonymousSinkLogicalOperator>(); anonymousSink.has_value())
            {
                newRoots.emplace_back(setAnonymousSink(testFileName, sltSinkProvider, currentQueryNumberInTest, anonymousSink.value()));
            }
            else if (auto namedSink = rootOperator.tryGetAs<SinkLogicalOperator>(); namedSink.has_value())
            {
                newRoots.emplace_back(
                    setNamedSink(currentBuilder, testFileName, sltSinkProvider, currentQueryNumberInTest, namedSink.value()));
            }
            else
            {
                throw UnsupportedQuery(
                    "Invalid root operator \"{}\". Root operators must be SinkLogicalOperators or AnonymousSinkLogicalOperators.",
                    rootOperator.getName());
            }
            plan = plan.withRootOperators(newRoots);
        }
    }

    void setAnonymousSinks(
        LogicalPlan& plan,
        const std::string_view& testFileName,
        SLTSinkFactory& sltSinkProvider,
        const SystestQueryId& currentQueryNumberInTest) const
    {
        std::vector<LogicalOperator> newRoots;


        for (const auto& rootOperator : plan.getRootOperators())
        {
            if (auto anonymousSink = rootOperator.tryGetAs<AnonymousSinkLogicalOperator>(); anonymousSink.has_value())
            {
                newRoots.emplace_back(setAnonymousSink(testFileName, sltSinkProvider, currentQueryNumberInTest, anonymousSink.value()));
            }
            else
            {
                newRoots.emplace_back(rootOperator);
            }
        }
        plan = plan.withRootOperators(newRoots);
    }

    [[nodiscard]] SystestQueryBuilder
    bindQueryStatement(const std::string_view& testFileName, SLTSinkFactory& sltSinkProvider, const Systest::QueryStatement& query) const
    {
        SystestQueryBuilder currentBuilder{query.id};
        currentBuilder.setQueryDefinition(query.sql);
        currentBuilder.setConfigurationOverrides({query.settings});
        currentBuilder.setExpectation(query.expected);
        if (query.sequential)
        {
            currentBuilder.setRunAfter(std::make_pair(TestName(testFileName), SystestQueryId{query.id.getRawValue() - 1}));
        }
        try
        {
            auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query.sql);
            setSinks(plan, currentBuilder, testFileName, sltSinkProvider, query.id);
            plan.setQueryId(QueryId::createDistributed(DistributedQueryId(fmt::format("{}:{}", testFileName, query.id))));
            setAnonymousSources(plan);
            currentBuilder.setBoundPlan(std::move(plan));
        }
        catch (Exception& e)
        {
            currentBuilder.setException(e);
        }

        return currentBuilder;
    }

    [[nodiscard]] SystestQueryBuilder bindExplainStatement(
        const StatementBinder& binder,
        const std::string_view& testFileName,
        SLTSinkFactory& sltSinkProvider,
        const Systest::ExplainStatement& statement) const
    {
        SystestQueryBuilder currentBuilder{statement.id};
        currentBuilder.setQueryDefinition(statement.sql);
        currentBuilder.setExpectation(ExpectedPlan{.lines = statement.expected, .qualifyingPrefix = {}});
        try
        {
            const auto managedParser = NES::AntlrSQLQueryParser::ManagedAntlrParser::create(statement.sql);
            const auto parseResult = managedParser->parseSingle();
            if (not parseResult.has_value())
            {
                throw InvalidQuerySyntax("failed to parse the statement \"{}\"", replaceAll(statement.sql, "\n", " "));
            }

            auto binding = binder.bind(parseResult.value().get());
            if (not binding.has_value())
            {
                throw InvalidQuerySyntax("failed to bind the statement \"{}\": {}", statement.sql, binding.error());
            }

            auto* explainStatement = std::get_if<ExplainQueryStatement>(&binding.value());
            if (explainStatement == nullptr)
            {
                throw UnsupportedQuery("expected an EXPLAIN statement, but got: \"{}\"", replaceAll(statement.sql, "\n", " "));
            }

            /// The inner query plan needs the same rewrites as a regular systest query, so that its anonymous sinks and
            /// sources resolve during optimization (the OPTIMIZED, DISTRIBUTED and ALL stages run the optimizer).
            setAnonymousSinks(explainStatement->plan, testFileName, sltSinkProvider, statement.id);
            setAnonymousSources(explainStatement->plan);
            currentBuilder.setExplainStatement(std::move(*explainStatement));
        }
        catch (Exception& e)
        {
            currentBuilder.setException(e);
        }

        return currentBuilder;
    }

    [[nodiscard]] SystestQueryBuilder bindDifferentialStatement(
        const std::string_view& testFileName, SLTSinkFactory& sltSinkProvider, const Systest::DifferentialStatement& statement) const
    {
        const auto differentialTestResultFileName = std::string(testFileName) + "differential";

        SystestQueryBuilder currentTest{statement.firstId};
        currentTest.setConfigurationOverrides({statement.settings});
        if (statement.sequential)
        {
            currentTest.setRunAfter(std::make_pair(TestName(testFileName), SystestQueryId{statement.firstId.getRawValue() - 1}));
        }

        try
        {
            auto leftPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(statement.firstSql);
            auto rightPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(statement.secondSql);

            setSinks(leftPlan, currentTest, testFileName, sltSinkProvider, statement.firstId);
            setSinks(rightPlan, currentTest, differentialTestResultFileName, sltSinkProvider, statement.firstId);

            setAnonymousSources(leftPlan);
            setAnonymousSources(rightPlan);

            leftPlan.setQueryId(QueryId::createDistributed(DistributedQueryId(fmt::format("{}:{}", testFileName, statement.firstId))));
            rightPlan.setQueryId(
                QueryId::createDistributed(DistributedQueryId(fmt::format("{}:{}-differential", testFileName, statement.firstId))));

            currentTest.setQueryDefinition(statement.firstSql);
            currentTest.setBoundPlan(std::move(leftPlan));
            currentTest.setDifferentialQueryPlan(std::move(rightPlan));
        }
        catch (Exception& e)
        {
            currentTest.setException(e);
        }

        return currentTest;
    }

    std::vector<SystestQueryBuilder> loadFromSLTFile(
        const std::filesystem::path& testFilePath,
        const std::string_view testFileName,
        const std::shared_ptr<NES::SourceCatalog>& sourceCatalog,
        const std::shared_ptr<ModelCatalog>& modelCatalog,
        SLTSinkFactory& sltSinkProvider)
    {
        std::shared_ptr<std::vector<std::jthread>> sourceThreads = std::make_shared<std::vector<std::jthread>>();
        SystestParser parser{};
        const auto binder = NES::StatementBinder{
            sourceCatalog, [](auto&& pH1) { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(pH1)>(pH1)); }};

        parser.registerSubstitutionRule(
            {.keyword = "TESTDATA", .ruleFunction = [&](std::string& substitute) { substitute = testDataDir; }});
        parser.registerSubstitutionRule(
            {.keyword = "CONFIG/",
             .ruleFunction = [&](std::string& substitute)
             {
                 substitute = configDir;
                 if (!substitute.empty() && substitute.back() != '/')
                 {
                     substitute.push_back('/');
                 }
             }});

        parser.loadString(NES::readTestFile(testFilePath));

        NES::TestFile parsedFile = [&]
        {
            try
            {
                return parseTestFile(parser, testFilePath);
            }
            catch (Exception& exception)
            {
                tryLogCurrentException();
                exception.what() += fmt::format("Could not successfully parse test file://{}", testFilePath.string());
                throw;
            }
        }();

        std::vector<SystestQueryBuilder> builders;
        for (auto& statement : parsedFile.statements)
        {
            std::visit(
                Overloaded{
                    [&](Systest::CreateStatement& create)
                    {
                        std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> testData;
                        if (create.attach.has_value())
                        {
                            /// This binder predates the attached-data variant and dies with the rewrite-upfront step,
                            /// so the variant folds back into the pair shape it still reads.
                            testData = std::visit(
                                Overloaded{
                                    [](InlineRows& inlined)
                                    { return std::make_pair(TestDataIngestionType::INLINE, std::move(inlined.rows)); },
                                    [](const AttachedFile& attachedFile)
                                    {
                                        return std::make_pair(
                                            TestDataIngestionType::FILE, std::vector<std::string>{attachedFile.path.string()});
                                    }},
                                *create.attach);
                        }
                        bindCreateStatement(
                            binder, sourceCatalog, modelCatalog, sltSinkProvider, sourceThreads, create.sql, std::move(testData));
                    },
                    [&](const Systest::QueryStatement& query)
                    { builders.push_back(bindQueryStatement(testFileName, sltSinkProvider, query)); },
                    [&](const Systest::ExplainStatement& statement)
                    { builders.push_back(bindExplainStatement(binder, testFileName, sltSinkProvider, statement)); },
                    [&](const Systest::DifferentialStatement& statement)
                    { builders.push_back(bindDifferentialStatement(testFileName, sltSinkProvider, statement)); }},
                statement);
        }

        for (auto& builder : builders)
        {
            builder.setPaths(testFilePath, workingDir);
            builder.setName(std::string{testFileName});
            builder.setAdditionalSourceThreads(sourceThreads);
        }
        return builders;
    }

private:
    std::filesystem::path workingDir;
    std::filesystem::path testDataDir;
    std::filesystem::path configDir;
    QueryOptimizerConfiguration queryOptimizerConfiguration;
    SystestClusterConfiguration clusterConfiguration;

    SharedPtr<WorkerCatalog> workerCatalog;
};

SystestBinder::SystestBinder(
    const std::filesystem::path& workingDir,
    const std::filesystem::path& testDataDir,
    const std::filesystem::path& configDir,
    const QueryOptimizerConfiguration& queryOptimizerConfiguration,
    SystestClusterConfiguration clusterConfiguration)
    : impl(std::make_unique<Impl>(workingDir, testDataDir, configDir, queryOptimizerConfiguration, std::move(clusterConfiguration)))
{
}

std::pair<std::vector<SystestQuery>, size_t> SystestBinder::loadOptimizeQueries(const DiscoveredTestFiles& discoveredTestFiles)
{
    return impl->loadOptimizeQueries(discoveredTestFiles);
}

SystestBinder::~SystestBinder() = default;
}
