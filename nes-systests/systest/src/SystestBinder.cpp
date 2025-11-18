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
#include <Identifiers/NESStrongType.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/InlineSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/InlineSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDataProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>
#include <SystestParser.hpp>
#include <SystestState.hpp>

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
                const auto sink = sinkCatalog->addSinkDescriptor(std::string{assignedSinkName}, schema, sinkType, std::move(config));
                if (not sink.has_value())
                {
                    return std::unexpected{SinkAlreadyExists("Failed to create file sink with assigned name {}", assignedSinkName)};
                }
                return sink.value();
            });
        return success;
    }

    std::optional<SinkDescriptor>
    getInlineSink(const Schema& schema, std::string_view sinkType, std::unordered_map<std::string, std::string> config)
    {
        return sinkCatalog->getInlineSink(schema, std::move(sinkType), std::move(config));
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

    void setOptimizedPlan(LogicalPlan optimizedPlan)
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
        this->sourcesToFilePathsAndCounts.emplace(std::move(sourceNamesToFilepathAndCountForQuery));
        const auto sinkOperatorOpt = this->optimizedPlan->getRootOperators().at(0).tryGetAs<SinkLogicalOperator>();
        INVARIANT(sinkOperatorOpt.has_value(), "The optimized plan should have a sink operator");
        INVARIANT(sinkOperatorOpt.value()->getSinkDescriptor().has_value(), "The sink operator should have a sink descriptor");
        if (toUpperCase(sinkOperatorOpt.value()->getSinkDescriptor().value().getSinkType()) /// NOLINT(bugprone-unchecked-optional-access)
            == "CHECKSUM")
        {
            sinkOutputSchema = SLTSinkFactory::checksumSchema;
        }
        else
        {
            sinkOutputSchema = this->optimizedPlan->getRootOperators().at(0).getOutputSchema();
        }
    }

    void setDifferentialQueryPlan(LogicalPlan differentialQueryPlan) { this->differentialQueryPlan = std::move(differentialQueryPlan); }

    void optimizeQueries(const NES::LegacyOptimizer& optimizer)
    {
        if (!boundPlan.has_value())
        {
            return;
        }
        try
        {
            setOptimizedPlan(optimizer.optimize(boundPlan.value()));
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
                auto optimizedDiff = optimizer.optimize(differentialQueryPlan.value());
                setDifferentialQueryPlan(std::move(optimizedDiff));
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
                 .differentialQueryPlan = differentialQueryPlan});
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
    std::optional<LogicalPlan> optimizedPlan;
    std::optional<std::unordered_map<SourceDescriptor, std::pair<SourceInputFile, uint64_t>>> sourcesToFilePathsAndCounts;
    std::optional<Schema> sinkOutputSchema;
    std::optional<std::variant<std::vector<std::string>, ExpectedError>> expectedResultsOrError;
    std::optional<std::shared_ptr<std::vector<std::jthread>>> additionalSourceThreads;
    std::vector<ConfigurationOverride> configurationOverrides{ConfigurationOverride{}};
    std::optional<LogicalPlan> differentialQueryPlan;
    bool built = false;
};

struct SystestBinder::Impl
{
    explicit Impl(std::filesystem::path workingDir, std::filesystem::path testDataDir, std::filesystem::path configDir)
        : workingDir(std::move(workingDir)), testDataDir(std::move(testDataDir)), configDir(std::move(configDir))
    {
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
        SLTSinkFactory sinkProvider{testfile.sinkCatalog};
        auto loadedSystests = loadFromSLTFile(testfile.file, testfile.name(), testfile.sourceCatalog, sinkProvider);
        std::unordered_set<SystestQueryId> foundQueries;

        const LegacyOptimizer optimizer{testfile.sourceCatalog, testfile.sinkCatalog};

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
            builder.optimizeQueries(optimizer);
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
            .logical = statement.attachedTo.getLogicalSourceName(),
            .type = statement.sourceType,
            .parserConfig = statement.parserConfig,
            .sourceConfig = statement.sourceConfig};

        std::unordered_map<std::string, std::string> defaultParserConfig{{"type", "CSV"}};
        physicalSourceConfig.parserConfig.merge(defaultParserConfig);

        if (testData.has_value())
        {
            physicalSourceConfig = setUpSourceWithTestData(physicalSourceConfig, sourceThreads, std::move(testData.value()));
        }


        if (const auto created = sourceCatalog->addPhysicalSource(
                statement.attachedTo, physicalSourceConfig.type, physicalSourceConfig.sourceConfig, physicalSourceConfig.parserConfig))
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
        else
        {
            throw UnsupportedQuery();
        }
    }

    [[nodiscard]] LogicalOperator updateInlineSource(const LogicalOperator& current) const
    {
        std::vector<LogicalOperator> newChildren;
        for (const auto& child : current.getChildren())
        {
            newChildren.emplace_back(updateInlineSource(child));
        }

        if (const auto inlineSource = current.tryGetAs<InlineSourceLogicalOperator>())
        {
            auto sourceConfig = inlineSource.value()->getSourceConfig();
            auto parserConfig = inlineSource.value()->getParserConfig();

            parserConfig.try_emplace("type", "CSV");

            /// By default, all relative paths are relative to the testDataDir.
            if (sourceConfig.contains("file_path") && !sourceConfig.at("file_path").starts_with("/"))
            {
                auto filePath = inlineSource.value()->getSourceConfig().at("file_path");
                filePath = testDataDir / filePath;
                sourceConfig.erase("file_path");
                sourceConfig.emplace("file_path", filePath);
            }

            if (sourceConfig != inlineSource.value()->getSourceConfig() || parserConfig != inlineSource.value()->getParserConfig())
            {
                const InlineSourceLogicalOperator newOperator{
                    inlineSource.value()->getSourceType(), inlineSource.value()->getSchema(), sourceConfig, parserConfig};

                return newOperator.withChildren(newChildren);
            }
        }

        return current.withChildren(std::move(newChildren));
    }

    void setInlineSources(LogicalPlan& plan) const
    {
        std::vector<LogicalOperator> newRoots;
        for (const auto& root : plan.getRootOperators())
        {
            newRoots.emplace_back(updateInlineSource(root));
        }
        plan = plan.withRootOperators(newRoots);
    }

    LogicalOperator setInlineSink(
        const std::string_view& testFileName,
        SLTSinkFactory& sltSinkProvider,
        const SystestQueryId& currentQueryNumberInTest,
        const TypedLogicalOperator<InlineSinkLogicalOperator>& sinkOperator) const
    {
        const auto resultFile = SystestQuery::resultFile(workingDir, testFileName, currentQueryNumberInTest);

        auto sinkConfig = sinkOperator->getSinkConfig();
        auto schema = sinkOperator->getSchema();
        sinkConfig.erase("file_path");
        sinkConfig.emplace("file_path", resultFile);

        if (sinkOperator->getSinkType() == "FILE")
        {
            sinkConfig.erase("input_format");
            sinkConfig.emplace("input_format", "CSV");
        }

        auto sinkDescriptor = sltSinkProvider.getInlineSink(schema, sinkOperator->getSinkType(), sinkConfig);
        if (not sinkDescriptor.has_value())
        {
            throw InvalidConfigParameter("Failed to create inline sink of type {}", sinkOperator->getSinkType());
        }
        const auto newOperator = SinkLogicalOperator{sinkDescriptor.value()};

        return newOperator.withChildren(sinkOperator->getChildren());
    }

    LogicalOperator setNamedSink(
        SystestQueryBuilder& currentBuilder,
        const std::string_view& testFileName,
        SLTSinkFactory& sltSinkProvider,
        const SystestQueryId& currentQueryNumberInTest,
        const TypedLogicalOperator<SinkLogicalOperator>& sinkOperator) const
    {
        const std::string sinkName = sinkOperator->getSinkName();

        /// Replacing the sinkName with the created unique sink name
        const auto sinkForQuery = toUpperCase(sinkName + std::to_string(currentQueryNumberInTest.getRawValue()));


        /// Adding the sink to the sink config, such that we can create a fully specified query plan
        const auto resultFile = SystestQuery::resultFile(workingDir, testFileName, currentQueryNumberInTest);

        auto sinkExpected = sltSinkProvider.createActualSink(toUpperCase(sinkName), sinkForQuery, resultFile);
        if (not sinkExpected.has_value())
        {
            currentBuilder.setException(sinkExpected.error());
        }

        const auto newOperator = SinkLogicalOperator{sinkExpected.value()};

        return newOperator.withChildren(sinkOperator->getChildren());
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
            if (auto inlineSink = rootOperator.tryGetAs<InlineSinkLogicalOperator>(); inlineSink.has_value())
            {
                newRoots.emplace_back(setInlineSink(testFileName, sltSinkProvider, currentQueryNumberInTest, inlineSink.value()));
            }
            else if (auto namedSink = rootOperator.tryGetAs<SinkLogicalOperator>(); namedSink.has_value())
            {
                newRoots.emplace_back(
                    setNamedSink(currentBuilder, testFileName, sltSinkProvider, currentQueryNumberInTest, namedSink.value()));
            }
            else
            {
                throw UnsupportedQuery(
                    "Invalid root operator \"{}\". Root operators must be SinkLogicalOperators or InlineSinksLogicalOperators.",
                    rootOperator.getName());
            }
            plan = plan.withRootOperators(newRoots);
        }
    }

    void queryCallback(
        const std::string_view& testFileName,
        std::unordered_map<SystestQueryId, SystestQueryBuilder>& plans,
        SLTSinkFactory& sltSinkProvider,
        const std::string& query,
        const SystestQueryId& currentQueryNumberInTest,
        const std::vector<ConfigurationOverride>& configOverrides) const
    {
        SystestQueryBuilder currentBuilder{currentQueryNumberInTest};
        currentBuilder.setQueryDefinition(query);
        currentBuilder.setConfigurationOverrides(configOverrides);
        try
        {
            auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
            setSinks(plan, currentBuilder, testFileName, sltSinkProvider, currentQueryNumberInTest);
            setInlineSources(plan);
            currentBuilder.setBoundPlan(std::move(plan));
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
        SystestQueryId& lastParsedQueryId,
        const std::string_view& testFileName,
        std::unordered_map<SystestQueryId, SystestQueryBuilder>& plans,
        SLTSinkFactory& sltSinkProvider,
        std::string leftQuery,
        std::string rightQuery,
        const SystestQueryId currentQueryNumberInTest,
        const std::vector<ConfigurationOverride>& configOverrides) const
    {
        const auto differentialTestResultFileName = std::string(testFileName) + "differential";

        auto& currentTest = plans.emplace(currentQueryNumberInTest, SystestQueryBuilder{currentQueryNumberInTest}).first->second;
        currentTest.setConfigurationOverrides(configOverrides);

        try
        {
            auto leftPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(leftQuery);
            auto rightPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(rightQuery);

            setSinks(leftPlan, currentTest, testFileName, sltSinkProvider, currentQueryNumberInTest);
            setSinks(rightPlan, currentTest, differentialTestResultFileName, sltSinkProvider, currentQueryNumberInTest);

            setInlineSources(leftPlan);
            setInlineSources(rightPlan);

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
        std::unordered_map<SystestQueryId, SystestQueryBuilder> plans{};
        std::shared_ptr<std::vector<std::jthread>> sourceThreads = std::make_shared<std::vector<std::jthread>>();
        const std::unordered_map<SourceDescriptor, std::filesystem::path> generatedDataPaths{};
        std::vector configOverrides{ConfigurationOverride{}};
        std::vector globalConfigOverrides{ConfigurationOverride{}};
        std::vector lastMergedConfigOverrides{ConfigurationOverride{}};
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

        if (!parser.loadFile(testFilePath))
        {
            throw TestException("Could not successfully load test file://{}", testFilePath.string());
        }

        SystestQueryId lastParsedQueryId = INVALID_SYSTEST_QUERY_ID;
        parser.registerOnQueryCallback(
            [&](std::string query, SystestQueryId currentQueryNumberInTest)
            {
                lastParsedQueryId = currentQueryNumberInTest;
                auto mergedConfigOverrides = mergeConfigurations(configOverrides, globalConfigOverrides);
                lastMergedConfigOverrides = mergedConfigOverrides;
                queryCallback(testFileName, plans, sltSinkProvider, std::move(query), currentQueryNumberInTest, mergedConfigOverrides);
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
                    std::move(currentQueryNumberInTest),
                    lastMergedConfigOverrides);
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
