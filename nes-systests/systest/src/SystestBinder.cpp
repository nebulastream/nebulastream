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
#include <Configurations/ConfigResolution.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Discovery/TestFileReader.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/AnonymousSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <OutputFormatters/CSVOutputFormatterConfig.hpp>
#include <Parser/SystestParser.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/FileSink.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/FileSourceConfig.hpp>
#include <Sources/SourceDataProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Files.hpp>
#include <Util/Logger/Logger.hpp>
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

#include <ChecksumSink.hpp>

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

    bool registerSink(const CreateSinkStatement& createSinkStatement)
    {
        /// Also register the sink under its declared name: EXPLAIN statements keep the original
        /// sink name in the plan, so the optimizer's sink binding must find it in the catalog.
        /// Executed queries instead go through createActualSink, which registers per-query sinks
        /// under assigned names.
        if (const auto sink = sinkCatalog->addSinkDescriptor(
                createSinkStatement.name,
                createSinkStatement.schema,
                createSinkStatement.generalSinkConfig,
                createSinkStatement.pluginSinkConfig,
                createSinkStatement.outputFormatterDescriptor);
            not sink.has_value())
        {
            throw sink.error();
        }
        auto [_, success] = sinkProviders.emplace(
            createSinkStatement.name,
            [this, createSinkStatement = createSinkStatement](
                Identifier assignedSinkName, const std::filesystem::path& filePath) -> std::expected<SinkDescriptor, Exception>
            {
                auto [name, schema, generalSinkConfig, pluginSinkConfig, outputFormatterConfig] = std::move(createSinkStatement);
                if (pluginSinkConfig.getPluginData().getUnderlying().type() == typeid(FileSinkConfig))
                {
                    auto fileSinkConfig = pluginSinkConfig.getPluginData().getAs<FileSinkConfig>();
                    fileSinkConfig.filePath = filePath;
                    pluginSinkConfig
                        = PluginSinkConfiguration(pluginSinkConfig.getType(), ExplicitAny{std::any{std::move(fileSinkConfig)}});
                }
                else if (pluginSinkConfig.getPluginData().getUnderlying().type() == typeid(ChecksumSinkConfig))
                {
                    auto checksumSinkConfig = pluginSinkConfig.getPluginData().getAs<ChecksumSinkConfig>();
                    checksumSinkConfig.filePath = filePath;
                    pluginSinkConfig
                        = PluginSinkConfiguration{pluginSinkConfig.getType(), ExplicitAny{std::any{std::move(checksumSinkConfig)}}};
                    if (outputFormatterConfig.getConfig().getUnderlying().type() == typeid(CSVOutputFormatterConfig))
                    {
                        auto csvOutputConfig = outputFormatterConfig.getConfig().getAs<CSVOutputFormatterConfig>();
                        csvOutputConfig.quoteStrings = true;
                        outputFormatterConfig = OutputFormatterDescriptor{
                            outputFormatterConfig.getOutputFormatterType(), ExplicitAny{std::any{std::move(csvOutputConfig)}}};
                    }
                }
                return sinkCatalog->addSinkDescriptor(
                    std::move(assignedSinkName),
                    schema,
                    std::move(generalSinkConfig),
                    std::move(pluginSinkConfig),
                    std::move(outputFormatterConfig));
            });
        return success;
    }

    std::optional<SinkDescriptor> getAnonymousSink(
        GeneralSinkConfig generalSinkConfig,
        PluginSinkConfiguration pluginSinkConfig,
        OutputFormatterDescriptor outputFormatterDescriptor,
        AnonymousSinkSchema schema)
    {
        return sinkCatalog->createAnonymousSinkDescriptor(
            std::move(schema), std::move(generalSinkConfig), std::move(pluginSinkConfig), std::move(outputFormatterDescriptor));
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
                /// getSourceDescriptor returns by value, assigning to keep the reference alive.
                const auto sourceDescriptor = logicalSourceOperator->getSourceDescriptor();
                if (sourceDescriptor.getPluginData().getUnderlying().has_value()
                    && sourceDescriptor.getPluginData().getUnderlying().type() == typeid(FileSourceConfig))
                {
                    if (auto entry = sourceNamesToFilepathAndCountForQuery.extract(sourceDescriptor); entry.empty())
                    {
                        const auto& path = sourceDescriptor.getPluginData().template getAs<const FileSourceConfig&>().filePath;
                        sourceNamesToFilepathAndCountForQuery.emplace(sourceDescriptor, std::make_pair(SourceInputFile{path}, 1));
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
                        sourceDescriptor.getPhysicalSourceId(),
                        sourceDescriptor.getLogicalSourceName());
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
                expectedResultsOrError.has_value() || differentialQueryPlan.has_value(),
                "Differential query plan or error has not been set");
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
                 .expectedResultsOrExpectedError = expectedResultsOrError.has_value()
                     ? expectedResultsOrError.value()
                     : std::variant<std::vector<std::string>, ExpectedError>{std::vector<std::string>{}},
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
        const auto expectedResultsValue = expectedResultsOrError.has_value()
            ? expectedResultsOrError.value()
            : std::variant<std::vector<std::string>, ExpectedError>{std::vector<std::string>{}};

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
                 .expectedResultsOrExpectedError = expectedResultsValue,
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
    std::optional<std::variant<std::vector<std::string>, ExpectedError>> expectedResultsOrError;
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
        SystestClusterConfiguration clusterConfiguration,
        std::function<AntlrSQLQueryParser::QueryBinder()> queryBinderFactory,
        std::function<StatementBinder(const std::shared_ptr<NES::SourceCatalog>&, AntlrSQLQueryParser::QueryBinder)> binderFactory)
        : workingDir(std::move(workingDir))
        , testDataDir(std::move(testDataDir))
        , configDir(std::move(configDir))
        , queryOptimizerConfiguration(std::move(queryOptimizerConfiguration))
        , clusterConfiguration(std::move(clusterConfiguration))
        , queryBinderFactory(std::move(queryBinderFactory))
        , statementBinderFactory(std::move(binderFactory))
    {
        this->workerCatalog = std::make_shared<WorkerCatalog>();
        for (const auto& [host, data, capacity, downstream, config] : this->clusterConfiguration.workers)
        {
            workerCatalog->addWorker(host, data, capacity, downstream, config);
        }
    }

    static std::vector<ConfigurationOverride>
    mergeConfigurations(const std::vector<ConfigurationOverride>& overrides, const std::vector<ConfigurationOverride>& otherOverrides)
    {
        const auto isDefault = [](const std::vector<ConfigurationOverride>& collection)
        { return collection.empty() || (collection.size() == 1 && collection.front().overrideParameters.empty()); };

        if (isDefault(overrides) && isDefault(otherOverrides))
        {
            return {ConfigurationOverride{}};
        }
        if (isDefault(overrides))
        {
            return otherOverrides;
        }
        if (isDefault(otherOverrides))
        {
            return overrides;
        }

        std::vector<ConfigurationOverride> combined;
        combined.reserve(overrides.size() * otherOverrides.size());

        for (const auto& override : overrides)
        {
            for (const auto& other : otherOverrides)
            {
                auto merged = other;
                for (const auto& [key, value] : override.overrideParameters)
                {
                    merged.overrideParameters[key] = value;
                }

                const bool alreadyPresent
                    = std::ranges::any_of(combined, [&merged](const ConfigurationOverride& existing) { return existing == merged; });
                if (!alreadyPresent)
                {
                    combined.emplace_back(std::move(merged));
                }
            }
        }

        if (combined.empty())
        {
            combined.emplace_back();
        }

        return combined;
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
        SLTSinkFactory sinkProvider{testfile.sinkCatalog, clusterConfiguration.allowSinkPlacement};
        auto modelCatalog = std::make_shared<ModelCatalog>();
        auto loadedSystests = loadFromSLTFile(testfile.file, testfile.name(), testfile.sourceCatalog, modelCatalog, sinkProvider);
        std::unordered_set<SystestQueryId> foundQueries;

        const QueryOptimizer queryOptimizer{
            queryOptimizerConfiguration, testfile.sourceCatalog, testfile.sinkCatalog, copyPtr(workerCatalog), modelCatalog};

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

    [[nodiscard]] std::filesystem::path generateSourceFilePath(const std::string& testData) const { return testDataDir / testData; }

    [[nodiscard]] PhysicalSourceConfig setUpSourceWithTestData(
        PhysicalSourceConfig& physicalSourceConfig,
        std::shared_ptr<std::vector<std::jthread>> sourceThreads,
        std::pair<TestDataIngestionType, std::vector<std::string>> testData) const
    {
        switch (testData.first)
        {
            case TestDataIngestionType::INLINE: {
                return SourceDataProvider::provideInlineDataSource(
                    std::move(physicalSourceConfig), std::move(testData.second), std::move(sourceThreads));
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

        PhysicalSourceConfig physicalSourceConfig{
            .generalSourceConfig = statement.generalSourceConfig,
            .pluginSourceConfig = statement.pluginSourceConfig,
            .pluginInputFormatterConfig = statement.pluginInputFormatterConfig};

        if (testData.has_value())
        {
            physicalSourceConfig = setUpSourceWithTestData(physicalSourceConfig, sourceThreads, std::move(testData.value()));
        }
        PhysicalSourceBuilder sourceBuilder{
            physicalSourceConfig.generalSourceConfig,
            physicalSourceConfig.pluginSourceConfig,
            physicalSourceConfig.pluginInputFormatterConfig,
            sourceCatalog};

        /// The policy-resolved host enters the config as the HOST literal, resolved by the catalog.
        if (const auto created = sourceCatalog->registerWithLogicalSource(std::move(sourceBuilder), statement.logicalSourceName);
            not created.has_value())
        {
            throw Exception(created.error());
        }
    }

    static void createSink(SLTSinkFactory& sltSinkProvider, const CreateSinkStatement& statement)
    {
        sltSinkProvider.registerSink(statement);
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

    void createCallback(
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
            throw InvalidQuerySyntax(
                "failed to to parse the query \"{}\" with error", replaceAll(query, "\n", " "), parseResult.error().what());
        }

        const auto binding = binder.bind(parseResult.value().get());
        if (not binding.has_value())
        {
            throw InvalidQuerySyntax(
                "failed to to parse the query \"{}\" with error {}", replaceAll(query, "\n", " "), binding.error().what());
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

    LogicalOperator setAnonymousSink(
        const std::string_view& testFileName,
        SLTSinkFactory& sltSinkProvider,
        const SystestQueryId& currentQueryNumberInTest,
        const TypedLogicalOperator<AnonymousSinkLogicalOperator>& sinkOperator) const
    {
        const auto resultFile = SystestQuery::resultFile(workingDir, testFileName, currentQueryNumberInTest);

        auto pluginData = sinkOperator->getPluginSinkConfiguration().getPluginData();
        if (pluginData.getUnderlying().type() == typeid(FileSinkConfig))
        {
            auto fileSinkConfig = pluginData.getAs<FileSinkConfig>();
            fileSinkConfig.filePath = resultFile;
            pluginData = ExplicitAny{std::any{std::move(fileSinkConfig)}};
        }
        else if (pluginData.getUnderlying().type() == typeid(ChecksumSinkConfig))
        {
            auto checksumSinkConfig = pluginData.getAs<ChecksumSinkConfig>();
            checksumSinkConfig.filePath = resultFile;
            pluginData = ExplicitAny{std::any{std::move(checksumSinkConfig)}};
        }
        auto pluginSinkConfig = PluginSinkConfiguration{sinkOperator->getPluginSinkConfiguration().getType(), std::move(pluginData)};

        auto sinkDescriptor = sltSinkProvider.getAnonymousSink(
            sinkOperator->getGeneralSinkConfig(),
            std::move(pluginSinkConfig),
            sinkOperator->getOutputFormatterDescriptor(),
            sinkOperator->getSinkSchema());
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

    void queryCallback(
        const std::string_view& testFileName,
        std::unordered_map<SystestQueryId, SystestQueryBuilder>& plans,
        SLTSinkFactory& sltSinkProvider,
        const std::string& query,
        const AntlrSQLQueryParser::QueryBinder& queryBinder,
        const SystestQueryId& currentQueryNumberInTest,
        const std::vector<ConfigurationOverride>& configOverrides,
        const bool sequentialExecution) const
    {
        SystestQueryBuilder currentBuilder{currentQueryNumberInTest};
        currentBuilder.setQueryDefinition(query);
        currentBuilder.setConfigurationOverrides(configOverrides);
        if (sequentialExecution)
        {
            currentBuilder.setRunAfter(std::make_pair(TestName(testFileName), SystestQueryId{currentQueryNumberInTest.getRawValue() - 1}));
        }
        try
        {
            auto plan = queryBinder.createLogicalQueryPlanFromSQLString(query);
            setSinks(plan, currentBuilder, testFileName, sltSinkProvider, currentQueryNumberInTest);
            plan.setQueryId(QueryId::createDistributed(DistributedQueryId(fmt::format("{}:{}", testFileName, currentQueryNumberInTest))));
            currentBuilder.setBoundPlan(std::move(plan));
        }
        catch (Exception& e)
        {
            currentBuilder.setException(e);
        }

        plans.emplace(currentQueryNumberInTest, currentBuilder);
    }

    void explainCallback(
        const StatementBinder& binder,
        const std::string_view& testFileName,
        std::unordered_map<SystestQueryId, SystestQueryBuilder>& plans,
        SLTSinkFactory& sltSinkProvider,
        const std::string& statementString,
        const SystestQueryId& currentQueryNumberInTest) const
    {
        SystestQueryBuilder currentBuilder{currentQueryNumberInTest};
        currentBuilder.setQueryDefinition(statementString);
        try
        {
            const auto managedParser = NES::AntlrSQLQueryParser::ManagedAntlrParser::create(statementString);
            const auto parseResult = managedParser->parseSingle();
            if (not parseResult.has_value())
            {
                throw InvalidQuerySyntax("failed to parse the statement \"{}\"", replaceAll(statementString, "\n", " "));
            }

            auto binding = binder.bind(parseResult.value().get());
            if (not binding.has_value())
            {
                throw InvalidQuerySyntax("failed to bind the statement \"{}\": {}", statementString, binding.error());
            }

            auto* explainStatement = std::get_if<ExplainQueryStatement>(&binding.value());
            if (explainStatement == nullptr)
            {
                throw UnsupportedQuery("expected an EXPLAIN statement, but got: \"{}\"", replaceAll(statementString, "\n", " "));
            }

            /// The inner query plan needs the same rewrites as a regular systest query, so that its anonymous sinks and
            /// sources resolve during optimization (the OPTIMIZED, DISTRIBUTED and ALL stages run the optimizer).
            setAnonymousSinks(explainStatement->plan, testFileName, sltSinkProvider, currentQueryNumberInTest);
            currentBuilder.setExplainStatement(std::move(*explainStatement));
        }
        catch (Exception& e)
        {
            currentBuilder.setException(e);
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
        SystestQueryId&,
        const std::string_view& testFileName,
        std::unordered_map<SystestQueryId, SystestQueryBuilder>& plans,
        SLTSinkFactory& sltSinkProvider,
        std::string leftQuery,
        std::string rightQuery,
        const AntlrSQLQueryParser::QueryBinder& queryBinder,
        const SystestQueryId currentQueryNumberInTest,
        const std::vector<ConfigurationOverride>& configOverrides) const
    {
        const auto differentialTestResultFileName = std::string(testFileName) + "differential";

        auto& currentTest = plans.emplace(currentQueryNumberInTest, SystestQueryBuilder{currentQueryNumberInTest}).first->second;


        currentTest.setConfigurationOverrides(configOverrides);

        try
        {
            auto leftPlan = queryBinder.createLogicalQueryPlanFromSQLString(leftQuery);
            auto rightPlan = queryBinder.createLogicalQueryPlanFromSQLString(rightQuery);

            setSinks(leftPlan, currentTest, testFileName, sltSinkProvider, currentQueryNumberInTest);
            setSinks(rightPlan, currentTest, differentialTestResultFileName, sltSinkProvider, currentQueryNumberInTest);

            leftPlan.setQueryId(
                QueryId::createDistributed(DistributedQueryId(fmt::format("{}:{}", testFileName, currentQueryNumberInTest))));
            rightPlan.setQueryId(
                QueryId::createDistributed(DistributedQueryId(fmt::format("{}:{}-differential", testFileName, currentQueryNumberInTest))));

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
        const std::shared_ptr<ModelCatalog>& modelCatalog,
        SLTSinkFactory& sltSinkProvider)
    {
        uint64_t sourceIndex = 0;
        std::unordered_map<SystestQueryId, SystestQueryBuilder> plans{};
        std::shared_ptr<std::vector<std::jthread>> sourceThreads = std::make_shared<std::vector<std::jthread>>();
        const std::unordered_map<SourceDescriptor, std::filesystem::path> generatedDataPaths{};
        std::vector configOverrides{ConfigurationOverride{}};
        std::vector globalConfigOverrides{ConfigurationOverride{}};
        std::vector lastMergedConfigOverrides{ConfigurationOverride{}};
        SystestParser parser{};
        const AntlrSQLQueryParser::QueryBinder queryBinder = queryBinderFactory();
        const auto binder = statementBinderFactory(sourceCatalog, queryBinder);

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

        if (!parser.loadString(NES::readTestFile(testFilePath)))
        {
            throw TestException("Could not successfully load test file://{}", testFilePath.string());
        }

        SystestQueryId lastParsedQueryId = INVALID_SYSTEST_QUERY_ID;
        parser.registerOnQueryCallback(
            [&](const std::string& query, SystestQueryId currentQueryNumberInTest, bool sequentialExecution)
            {
                lastParsedQueryId = currentQueryNumberInTest;
                auto mergedConfigOverrides = mergeConfigurations(configOverrides, globalConfigOverrides);
                lastMergedConfigOverrides = mergedConfigOverrides;
                queryCallback(
                    testFileName,
                    plans,
                    sltSinkProvider,
                    query,
                    queryBinder,
                    currentQueryNumberInTest,
                    mergedConfigOverrides,
                    sequentialExecution);
                configOverrides = {ConfigurationOverride{}};
            });

        parser.registerOnExplainQueryCallback(
            [&](const std::string& statement, SystestQueryId currentQueryNumberInTest)
            {
                explainCallback(binder, testFileName, plans, sltSinkProvider, statement, currentQueryNumberInTest);
                /// EXPLAIN statements are not executed, so configuration overrides do not apply; reset them so they
                /// do not leak into the next query.
                configOverrides = {ConfigurationOverride{}};
            });

        parser.registerOnErrorExpectationCallback(
            [&](SystestParser::ErrorExpectation errorExpectation, SystestQueryId correspondingQueryId)
            { errorExpectationCallback(plans, std::move(errorExpectation), std::move(correspondingQueryId)); });

        parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, SystestQueryId correspondingQueryId)
                                              { resultTuplesCallback(plans, std::move(resultTuples), std::move(correspondingQueryId)); });

        parser.registerOnConfigurationCallback(
            [&](const std::vector<ConfigurationOverride>& overrides)
            {
                const bool isDefault = configOverrides.size() == 1 && configOverrides.front().overrideParameters.empty();
                if (isDefault)
                {
                    configOverrides = overrides;
                }
                else
                {
                    configOverrides = mergeConfigurations(overrides, configOverrides);
                }
            });

        parser.registerOnGlobalConfigurationCallback(
            [&](const std::vector<ConfigurationOverride>& overrides)
            {
                const bool isDefault = globalConfigOverrides.size() == 1 && globalConfigOverrides.front().overrideParameters.empty();
                if (isDefault)
                {
                    globalConfigOverrides = overrides;
                }
                else
                {
                    globalConfigOverrides = mergeConfigurations(overrides, globalConfigOverrides);
                }
            });

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
                    queryBinder,
                    std::move(currentQueryNumberInTest),
                    lastMergedConfigOverrides);
            });

        parser.registerOnCreateCallback(
            [&, sourceCatalog, modelCatalog](
                const std::string& query, std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> input)
            { createCallback(binder, sourceCatalog, modelCatalog, sltSinkProvider, sourceThreads, query, std::move(input)); });

        try
        {
            parser.parse();
        }
        catch (Exception& exception)
        {
            tryLogCurrentException();
            exception.what() += fmt::format("Could not successfully parse and bind test file://{}", testFilePath.string());
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
    QueryOptimizerConfiguration queryOptimizerConfiguration;
    SystestClusterConfiguration clusterConfiguration;

    /// Add file-specific dependencies here
    std::function<AntlrSQLQueryParser::QueryBinder()> queryBinderFactory;
    std::function<StatementBinder(const std::shared_ptr<NES::SourceCatalog>&, AntlrSQLQueryParser::QueryBinder)> statementBinderFactory;


    SharedPtr<WorkerCatalog> workerCatalog;
};

SystestBinder::SystestBinder(
    const std::filesystem::path& workingDir,
    const std::filesystem::path& testDataDir,
    const std::filesystem::path& configDir,
    const QueryOptimizerConfiguration& queryOptimizerConfiguration,
    SystestClusterConfiguration clusterConfiguration,
    std::function<AntlrSQLQueryParser::QueryBinder()> queryBinderFactory,
    std::function<StatementBinder(const std::shared_ptr<NES::SourceCatalog>&, AntlrSQLQueryParser::QueryBinder)> statementBinderFactory)
    : impl(std::make_unique<Impl>(
          workingDir,
          testDataDir,
          configDir,
          queryOptimizerConfiguration,
          std::move(clusterConfiguration),
          std::move(queryBinderFactory),
          std::move(statementBinderFactory)))
{
}

std::pair<std::vector<SystestQuery>, size_t> SystestBinder::loadOptimizeQueries(const TestFileMap& discoveredTestFiles)
{
    return impl->loadOptimizeQueries(discoveredTestFiles);
}

SystestBinder::~SystestBinder() = default;
}
