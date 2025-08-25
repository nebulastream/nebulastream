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
#include <InputFormatters/InputFormatterProvider.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDataProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <SystestSources/SystestSourceYAMLBinder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <GeneratorFields.hpp>
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
                std::unordered_map<std::string, std::string> config{{"filePath", std::move(filePath)}};
                if (sinkType == "File")
                {
                    config["inputFormat"] = "CSV";
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

    void setOptimizedPlan(LogicalPlan optimizedPlan)
    {
        this->optimizedPlan = std::move(optimizedPlan);
        std::unordered_map<SourceDescriptor, std::pair<SourceInputFile, uint64_t>> sourceNamesToFilepathAndCountForQuery;
        std::ranges::for_each(
            getOperatorByType<SourceDescriptorLogicalOperator>(*this->optimizedPlan),
            [&sourceNamesToFilepathAndCountForQuery](const SourceDescriptorLogicalOperator& logicalSourceOperator)
            {
                if (const auto path = logicalSourceOperator.getSourceDescriptor().tryGetFromConfig<std::string>(std::string{"filePath"});
                    path.has_value())
                {
                    if (auto entry = sourceNamesToFilepathAndCountForQuery.extract(logicalSourceOperator.getSourceDescriptor());
                        entry.empty())
                    {
                        sourceNamesToFilepathAndCountForQuery.emplace(
                            logicalSourceOperator.getSourceDescriptor(), std::make_pair(SourceInputFile{*path}, 1));
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
                        logicalSourceOperator.getSourceDescriptor().getPhysicalSourceId(),
                        logicalSourceOperator.getSourceDescriptor().getLogicalSource().getLogicalSourceName());
                }
            });
        this->sourcesToFilePathsAndCounts = std::move(sourceNamesToFilepathAndCountForQuery);
        const auto sinkOperatorOpt = this->optimizedPlan->getRootOperators().at(0).tryGet<SinkLogicalOperator>();
        INVARIANT(sinkOperatorOpt.has_value(), "The optimized plan should have a sink operator");
        INVARIANT(sinkOperatorOpt.value().getSinkDescriptor().has_value(), "The sink operator should have a sink descriptor");
        if (sinkOperatorOpt.value().getSinkDescriptor().value().getSinkType() == "Checksum") /// NOLINT(bugprone-unchecked-optional-access)
        {
            sinkOutputSchema = SLTSinkFactory::checksumSchema;
        }
        else
        {
            sinkOutputSchema = this->optimizedPlan->getRootOperators().at(0).getOutputSchema();
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
        PRECONDITION(expectedResultsOrError.has_value(), "Expected results or error has not been set");
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
        return SystestQuery{
            .testName = std::move(testName.value()),
            .queryIdInFile = queryIdInFile,
            .testFilePath = std::move(testFilePath.value()),
            .workingDir = std::move(workingDir.value()),
            .queryDefinition = std::move(queryDefinition.value()),
            .planInfoOrException = createPlanInfoOrException(),
            .expectedResultsOrExpectedError = std::move(expectedResultsOrError.value()),
            .additionalSourceThreads = std::move(additionalSourceThreads.value())};
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
    bool built = false;
};

struct SystestBinder::Impl
{
    explicit Impl(std::filesystem::path workingDir, std::filesystem::path testDataDir, std::filesystem::path configDir)
        : workingDir(std::move(workingDir)), testDataDir(std::move(testDataDir)), configDir(std::move(configDir))
    {
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
        std::cout << "Loaded test files: " << loadedFiles << "/" << discoveredTestFiles.size() << '\n' << std::flush;
        return std::make_pair(queries, loadedFiles);
    }

    std::vector<SystestQuery> loadOptimizeQueriesFromTestFile(const Systest::TestFile& testfile)
    {
        SLTSinkFactory sinkProvider{testfile.sinkCatalog};
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
                                 [&testfile, &foundQueries](auto& systest)
                                 {
                                     foundQueries.insert(systest.getSystemTestQueryId());

                                     if (systest.getBoundPlan().has_value())
                                     {
                                         const LegacyOptimizer optimizer{testfile.sourceCatalog, testfile.sinkCatalog};
                                         try
                                         {
                                             systest.setOptimizedPlan(optimizer.optimize(systest.getBoundPlan().value()));
                                         }
                                         catch (const Exception& exception)
                                         {
                                             systest.setException(exception);
                                         }
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
        SLTSinkFactory& sltSinkProvider)
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
                if (not contains(attachSource.inputFormatterType))
                {
                    throw UnknownSourceFormat("Did not find input formatter for type {}", attachSource.inputFormatterType);
                }

                /// Load physical source from file and overwrite logical source name with value from attach source
                const auto initialPhysicalSourceConfigFromFile = [](const std::string& logicalSourceName,
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
                };

                /// load physical source from inline definition
                const auto initialPhysicalSourceConfigInline
                    = [&sourceCatalog](const std::string& logicalSourceName, const InlineGeneratorConfiguration& inlineConfig)
                {
                    SystestSourceYAMLBinder::PhysicalSource physicalSource{};
                    physicalSource.logical = logicalSourceName;
                    physicalSource.parserConfig["type"] = "CSV";
                    physicalSource.parserConfig["fieldDelimiter"] = ",";
                    auto logicalSourceMapping = sourceCatalog.getLogicalSource(logicalSourceName);
                    if (!logicalSourceMapping.has_value())
                    {
                        throw InvalidConfigParameter("{} does not exist in the same catalog!", logicalSourceName);
                    }
                    auto definedLogicalSchema = logicalSourceMapping.value().getSchema();
                    if (definedLogicalSchema->getFields().size() != inlineConfig.fieldSchema.size())
                    {
                        throw InvalidConfigParameter(
                            "Number of defined generator field schemas ({}) does not match the number of fields defined in the source ({})",
                            definedLogicalSchema->getFields().size(),
                            inlineConfig.fieldSchema.size());
                    }

                    std::string generatorSchema;
                    for (const auto& fieldSchema : inlineConfig.fieldSchema)
                    {
                        auto fieldSchemaTokens = fieldSchema | std::views::split(' ')
                            | std::views::filter([](auto v) { return !v.empty(); })
                            | std::views::transform([](auto v) { return std::string_view(&*v.begin(), std::ranges::distance(v)); })
                            | std::ranges::to<std::vector<std::string>>();
                        auto fieldName = fieldSchemaTokens[0];
                        auto schemaFieldType = fieldSchemaTokens[2];
                        auto definedLogicalField = definedLogicalSchema->getFieldByName(fieldName);
                        if (!definedLogicalField.has_value())
                        {
                            throw InvalidConfigParameter(
                                "Field {} is defined in the generatorSchema, but does not exist in the sources logical schema!", fieldName);
                        }
                        if (magic_enum::enum_cast<DataType::Type>(schemaFieldType) != definedLogicalField.value().dataType.type)
                        {
                            throw InvalidConfigParameter(
                                "Field \"{}\" type in generator Schema does not match declared type ({}) in Source Schema ({})",
                                fieldName,
                                schemaFieldType,
                                magic_enum::enum_name<DataType::Type>(definedLogicalField.value().dataType.type));
                        }
                        auto generatorFieldIdentifier = fieldSchemaTokens[1];
                        auto [acceptedTypesBegin, acceptedTypesEnd]
                            = GeneratorFields::FieldNameToAcceptedTypes.equal_range(generatorFieldIdentifier);
                        bool isAcceptedType = false;
                        for (auto it = acceptedTypesBegin; it != acceptedTypesEnd; ++it)
                        {
                            if (definedLogicalField->dataType.type == it->second)
                            {
                                isAcceptedType = true;
                                break;
                            }
                        }
                        if (!isAcceptedType)
                        {
                            throw InvalidConfigParameter(
                                "Field {} is of {} type, which does not allow {}!",
                                fieldName,
                                generatorFieldIdentifier,
                                magic_enum::enum_name(definedLogicalField.value().dataType.type));
                        }

                        for (const auto& token : fieldSchemaTokens | std::views::drop(1))
                        {
                            generatorSchema.append(token);
                            generatorSchema.append(" "sv);
                        }
                        generatorSchema.back() = '\n';
                    }

                    physicalSource.sourceConfig = inlineConfig.options;
                    physicalSource.type = "Generator";
                    physicalSource.sourceConfig["generatorSchema"] = generatorSchema;
                    return physicalSource;
                };

                const auto initialPhysicalSourceConfig = attachSource.inlineGeneratorConfiguration.has_value()
                    ? initialPhysicalSourceConfigInline(attachSource.logicalSourceName, attachSource.inlineGeneratorConfiguration.value())
                    : initialPhysicalSourceConfigFromFile(
                          attachSource.logicalSourceName,
                          attachSource.sourceConfigurationPath,
                          attachSource.inputFormatterConfigurationPath);

                const auto [logical, sourceType, parserConfig, sourceConfig] = [&]()
                {
                    switch (attachSource.testDataIngestionType)
                    {
                        case TestDataIngestionType::INLINE: {
                            if (attachSource.tuples.has_value())
                            {
                                const auto sourceFile = SystestQuery::sourceFile(workingDir, testFileName, sourceIndex++);
                                return SourceDataProvider::provideInlineDataSource(initialPhysicalSourceConfig, attachSource, sourceFile);
                            }
                            throw CannotLoadConfig("An InlineData source must have tuples, but tuples was null.");
                        }
                        case TestDataIngestionType::FILE: {
                            return SourceDataProvider::provideFileDataSource(initialPhysicalSourceConfig, attachSource, testDataDir);
                        }
                        case TestDataIngestionType::GENERATOR: {
                            return SourceDataProvider::provideGeneratorDataSource(initialPhysicalSourceConfig, attachSource);
                        }
                    }
                    std::unreachable();
                }();

                const auto logicalSource = sourceCatalog.getLogicalSource(attachSource.logicalSourceName);
                if (not logicalSource.has_value())
                {
                    throw UnknownSourceName("{}", attachSource.logicalSourceName);
                }

                const auto physicalSource
                    = sourceCatalog.addPhysicalSource(logicalSource.value(), sourceType, sourceConfig, ParserConfig::create(parserConfig));
                if (not physicalSource.has_value())
                {
                    NES_ERROR(
                        "Concurrent deletion of just created logical source \"{}\" by another thread",
                        logicalSource.value().getLogicalSourceName());
                    throw UnknownSourceName(
                        "Failed to attach physical source with type {} to logical source {}",
                        attachSource.sourceType,
                        attachSource.logicalSourceName);
                }
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
                currentTest.setQueryDefinition(query);
                if (auto sinkExpected = sltSinkProvider.createActualSink(sinkName, sinkForQuery, resultFile); not sinkExpected.has_value())
                {
                    currentTest.setException(sinkExpected.error());
                }
                else
                {
                    try
                    {
                        auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
                        currentTest.setBoundPlan(std::move(plan));
                    }
                    catch (Exception& e)
                    {
                        currentTest.setException(e);
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

std::pair<std::vector<SystestQuery>, size_t> SystestBinder::loadOptimizeQueries(const TestFileMap& discoveredTestFiles)
{
    return impl->loadOptimizeQueries(discoveredTestFiles);
}

SystestBinder::~SystestBinder() = default;

}
