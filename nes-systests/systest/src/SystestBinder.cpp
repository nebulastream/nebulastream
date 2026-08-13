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
#include <Discovery/TestDiscovery.hpp>
#include <Discovery/TestFileReader.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Model/Expectation.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/AnonymousSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <OutputFormatters/CSVOutputFormatterConfig.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileBuilder.hpp>
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

#include <ChecksumSink.hpp>

namespace NES
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

    std::pair<std::vector<SystestQuery>, size_t> loadOptimizeQueries(const std::vector<DiscoveredTestFile>& discoveredTestFiles)
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
        /// Each test file declares its sources and sinks in its own terms, so each one binds against its own catalogs.
        auto sourceCatalogHandle = SourceCatalog::create();
        const auto sourceCatalog = copyPtr(sourceCatalogHandle);
        const auto sinkCatalog = std::make_shared<SinkCatalog>();

        SLTSinkFactory sinkProvider{sinkCatalog, clusterConfiguration.allowSinkPlacement};
        auto modelCatalog = std::make_shared<ModelCatalog>();
        auto loadedSystests = loadFromSLTFile(testfile.file, testfile.name().view(), sourceCatalog, modelCatalog, sinkProvider);
        std::unordered_set<SystestQueryId> foundQueries;

        const QueryOptimizer queryOptimizer{queryOptimizerConfiguration, sourceCatalog, sinkCatalog, copyPtr(workerCatalog), modelCatalog};

        std::vector<SystestQuery> buildSystests;
        for (auto& builder : loadedSystests)
        {
            const bool includeBuilder
                = not testfile.enabledQueries.has_value() || testfile.enabledQueries->contains(builder.getSystemTestQueryId());
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
        if (testfile.enabledQueries.has_value())
        {
            std::ranges::for_each(
                *testfile.enabledQueries
                    | std::views::filter([&foundQueries](const SystestQueryId testNumber)
                                         { return not foundQueries.contains(testNumber); }),
                [&testfile](const auto badTestNumber)
                {
                    std::cerr << fmt::format(
                        "Warning: Query number {} specified via command line argument but not found in file://{}",
                        badTestNumber,
                        testfile.file.string());
                });
        }

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

    [[nodiscard]] SystestQueryBuilder bindSelectStatement(
        const AntlrSQLQueryParser::QueryBinder& queryBinder,
        const std::string_view& testFileName,
        SLTSinkFactory& sltSinkProvider,
        const SelectStatement& query) const
    {
        SystestQueryBuilder currentBuilder{query.id};
        currentBuilder.setQueryDefinition(query.sql);
        currentBuilder.setConfigurationOverrides({query.overrides});
        currentBuilder.setExpectation(query.expected);
        try
        {
            auto plan = queryBinder.createLogicalQueryPlanFromSQLString(query.sql);
            setSinks(plan, currentBuilder, testFileName, sltSinkProvider, query.id);
            plan.setQueryId(QueryId::createDistributed(DistributedQueryId(fmt::format("{}:{}", testFileName, query.id))));
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
        const ExplainStatement& statement) const
    {
        SystestQueryBuilder currentBuilder{statement.id};
        currentBuilder.setQueryDefinition(statement.sql);
        currentBuilder.setExpectation(statement.expected);
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
            currentBuilder.setExplainStatement(std::move(*explainStatement));
        }
        catch (Exception& e)
        {
            currentBuilder.setException(e);
        }

        return currentBuilder;
    }

    [[nodiscard]] SystestQueryBuilder bindDifferentialStatement(
        const AntlrSQLQueryParser::QueryBinder& queryBinder,
        const std::string_view& testFileName,
        SLTSinkFactory& sltSinkProvider,
        const DifferentialStatement& statement) const
    {
        const auto differentialTestResultFileName = std::string(testFileName) + "differential";

        SystestQueryBuilder currentTest{statement.firstId};
        currentTest.setConfigurationOverrides({statement.overrides});
        try
        {
            auto leftPlan = queryBinder.createLogicalQueryPlanFromSQLString(statement.firstSql);
            auto rightPlan = queryBinder.createLogicalQueryPlanFromSQLString(statement.secondSql);

            setSinks(leftPlan, currentTest, testFileName, sltSinkProvider, statement.firstId);
            setSinks(rightPlan, currentTest, differentialTestResultFileName, sltSinkProvider, statement.firstId);

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

        parser.loadString(NES::readTestFile(testFilePath));

        NES::ParsedTestFile parsedFile = [&]
        {
            try
            {
                return buildTestFile(parser, testFilePath);
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
                    [&](CreateStatement& create)
                    {
                        std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> testData;
                        if (create.attach.has_value())
                        {
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
                    [&](const SelectStatement& query)
                    { builders.push_back(bindSelectStatement(queryBinder, testFileName, sltSinkProvider, query)); },
                    [&](const ExplainStatement& statement)
                    { builders.push_back(bindExplainStatement(binder, testFileName, sltSinkProvider, statement)); },
                    [&](const DifferentialStatement& statement)
                    { builders.push_back(bindDifferentialStatement(queryBinder, testFileName, sltSinkProvider, statement)); }},
                statement);
        }

        for (auto& builder : builders)
        {
            builder.setPaths(testFilePath, workingDir);
            builder.setName(TestName{std::string{testFileName}});
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

std::pair<std::vector<SystestQuery>, size_t> SystestBinder::loadOptimizeQueries(const std::vector<DiscoveredTestFile>& discoveredTestFiles)
{
    return impl->loadOptimizeQueries(discoveredTestFiles);
}

SystestBinder::~SystestBinder() = default;
}
