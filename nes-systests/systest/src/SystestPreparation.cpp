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

#include <SystestPreparation.hpp>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/AnonymousSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDataProvider.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Files.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <QueryId.hpp>
#include <QueryOptimizer.hpp>
#include <WorkerCatalog.hpp>

namespace NES::Systest
{
namespace
{

void configureNamedSinkOutput(
    const Identifier& sinkType,
    std::unordered_map<Identifier, std::string>& sinkConfig,
    std::unordered_map<Identifier, std::string>& formatConfig,
    const std::filesystem::path& outputFile)
{
    const auto filePath = Identifier::parse("file_path");
    const auto outputFormat = Identifier::parse("output_format");
    if (sinkType == Identifier::parse("File") || sinkType == Identifier::parse("CHECKSUM"))
    {
        sinkConfig.insert_or_assign(filePath, outputFile.string());
    }
    if (sinkType == Identifier::parse("File") && !sinkConfig.contains(outputFormat))
    {
        sinkConfig.emplace(outputFormat, "CSV");
    }
    if (sinkType == Identifier::parse("CHECKSUM"))
    {
        formatConfig.try_emplace(Identifier::parse("quote_strings"), "true");
    }
}

class SLTSinkFactory
{
public:
    SLTSinkFactory(std::shared_ptr<SinkCatalog> sinkCatalog, std::vector<Host> possibleSinkPlacements)
        : sinkCatalog(std::move(sinkCatalog)), possibleSinkPlacements(std::move(possibleSinkPlacements))
    {
    }

    void registerSink(const CreateSinkStatement& statement)
    {
        PRECONDITION(
            statement.host.has_value() || !possibleSinkPlacements.empty(),
            "Topology must list at least one worker in allow_sink_placement to assign a default sink host");
        const auto host = statement.host ? *statement.host : Host(possibleSinkPlacements.front().getRawValue());
        auto sinkConfig = statement.sinkConfig;
        auto formatConfig = statement.formatConfig;
        configureNamedSinkOutput(statement.sinkType, sinkConfig, formatConfig, "/tmp/none.txt");
        const auto sink = sinkCatalog->addSinkDescriptor(
            statement.name, statement.schema, statement.sinkType, host, std::move(sinkConfig), formatConfig);
        if (!sink)
        {
            throw Exception(sink.error());
        }

        sinkProviders.try_emplace(
            statement.name,
            [sinkCatalog = sinkCatalog,
             schema = statement.schema,
             sinkType = statement.sinkType,
             host,
             sinkConfig = statement.sinkConfig,
             formatConfig = statement.formatConfig](
                Identifier assignedSinkName, const std::filesystem::path& filePath) -> std::expected<SinkDescriptor, Exception>
            {
                auto actualConfig = sinkConfig;
                auto actualFormatConfig = formatConfig;
                configureNamedSinkOutput(sinkType, actualConfig, actualFormatConfig, filePath);
                return sinkCatalog->addSinkDescriptor(
                    std::move(assignedSinkName), schema, sinkType, host, std::move(actualConfig), actualFormatConfig);
            });
    }

    [[nodiscard]] std::optional<SinkDescriptor> getAnonymousSink(
        const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
        const Identifier& sinkType,
        std::unordered_map<Identifier, std::string> config,
        const std::unordered_map<Identifier, std::string>& formatConfig) const
    {
        PRECONDITION(
            !possibleSinkPlacements.empty(),
            "Topology must list at least one worker in allow_sink_placement to assign a default anonymous sink host");
        return sinkCatalog->getAnonymousSink(
            schema, sinkType, Host(possibleSinkPlacements.front().getRawValue()), std::move(config), formatConfig);
    }

    [[nodiscard]] std::expected<SinkDescriptor, Exception>
    createActualSink(const Identifier& sinkNameInFile, Identifier assignedSinkName, const std::filesystem::path& filePath) const
    {
        const auto provider = sinkProviders.find(sinkNameInFile);
        if (provider == sinkProviders.end())
        {
            throw UnknownSinkName("{}", sinkNameInFile);
        }
        return provider->second(std::move(assignedSinkName), filePath);
    }

    static inline const ResultSchema checksumSchema = []
    {
        return ResultSchema{std::vector{
            UnqualifiedUnboundField{Identifier::parse("COUNT"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            UnqualifiedUnboundField{Identifier::parse("CHECKSUM"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}}};
    }();

private:
    std::shared_ptr<SinkCatalog> sinkCatalog;
    std::vector<Host> possibleSinkPlacements;
    std::unordered_map<Identifier, std::function<std::expected<SinkDescriptor, Exception>(Identifier, std::filesystem::path)>>
        sinkProviders;
};

std::string encodePathComponent(const std::string_view component)
{
    static constexpr std::string_view HexDigits = "0123456789ABCDEF";
    std::string encoded;
    for (const auto character : component)
    {
        const auto byte = static_cast<unsigned char>(character);
        if (std::isalnum(byte) != 0 || character == '-' || character == '_')
        {
            encoded.push_back(character);
        }
        else
        {
            encoded.push_back('%');
            encoded.push_back(HexDigits[byte >> 4]);
            encoded.push_back(HexDigits[byte & 0x0F]);
        }
    }
    return encoded.empty() ? "_" : encoded;
}

std::filesystem::path encodedTestPath(const std::filesystem::path& relativeTestFile)
{
    std::filesystem::path encoded;
    for (const auto& component : relativeTestFile)
    {
        encoded /= encodePathComponent(component.generic_string());
    }
    return encoded.empty() ? std::filesystem::path{"_"} : encoded;
}

std::string executionIdentity(const TestCaseId& id, const std::string_view role)
{
    static constexpr std::string_view HexDigits = "0123456789ABCDEF";
    std::string pathIdentity;
    for (const auto character : id.source.relativeTestFile.generic_string())
    {
        const auto byte = static_cast<unsigned char>(character);
        pathIdentity.push_back(HexDigits[byte >> 4]);
        pathIdentity.push_back(HexDigits[byte & 0x0F]);
    }
    return fmt::format("{}_Q{}_V{}_{}", pathIdentity, id.source.queryNumber, id.configurationVariant, role);
}

std::filesystem::path resultFile(
    const std::filesystem::path& workingDirectory, const TestCaseId& id, const std::string_view role)
{
    const auto resultDirectory = workingDirectory / "results" / encodedTestPath(id.source.relativeTestFile);
    if (!is_directory(resultDirectory))
    {
        create_directories(resultDirectory);
        std::cout << "Created working directory: file://" << resultDirectory.string() << "\n";
    }
    return resultDirectory
        / fmt::format("query-{}-variant-{}-{}.csv", id.source.queryNumber, id.configurationVariant, role);
}

std::filesystem::path generatedSourceFile(const std::filesystem::path& workingDirectory)
{
    const auto sourceDirectory = workingDirectory / "sources";
    if (!is_directory(sourceDirectory))
    {
        create_directory(sourceDirectory);
        std::cout << "Created sources directory: file://" << sourceDirectory.string() << "\n";
    }
    return createUniqueFile(fmt::format("{}/input", sourceDirectory), ".csv").second;
}

void createLogicalSource(const std::shared_ptr<SourceCatalog>& sourceCatalog, const CreateLogicalSourceStatement& statement)
{
    if (!sourceCatalog->addLogicalSource(statement.name, statement.schema))
    {
        throw InvalidQuerySyntax();
    }
}

void createPhysicalSource(
    const std::shared_ptr<SourceCatalog>& sourceCatalog,
    const std::shared_ptr<std::vector<std::jthread>>& sourceThreads,
    const CreatePhysicalSourceStatement& statement,
    const std::optional<SourceDataSpec>& attachment,
    const SystestClusterConfiguration& cluster,
    const std::filesystem::path& workingDirectory,
    const std::filesystem::path& testDataDirectory,
    std::vector<std::filesystem::path>& generatedAttachments)
{
    if (!statement.host && cluster.allowSourcePlacement.empty())
    {
        throw InvalidConfigParameter(
            "Topology must list at least one worker in allow_source_placement to assign a default source host");
    }
    const auto host = statement.host ? *statement.host : Host(cluster.allowSourcePlacement.front().getRawValue());
    PhysicalSourceConfig physicalSourceConfig{
        .logical = statement.attachedTo.asCanonicalString(),
        .type = statement.sourceType,
        .parserConfig = statement.parserConfig,
        .sourceConfig = statement.sourceConfig};
    std::unordered_map<Identifier, std::string> defaultParserConfig{{Identifier::parse("type"), "CSV"}};
    physicalSourceConfig.parserConfig.merge(defaultParserConfig);

    if (const auto* inlineData = attachment ? std::get_if<InlineSourceData>(&*attachment) : nullptr)
    {
        auto file = generatedSourceFile(workingDirectory);
        generatedAttachments.push_back(file);
        physicalSourceConfig = SourceDataProvider::provideInlineDataSource(
            std::move(physicalSourceConfig), inlineData->rows, sourceThreads, std::move(file));
    }
    else if (const auto* fileData = attachment ? std::get_if<FileSourceData>(&*attachment) : nullptr)
    {
        const auto file = testDataDirectory / fileData->file;
        physicalSourceConfig = SourceDataProvider::provideFileDataSource(std::move(physicalSourceConfig), sourceThreads, std::move(file));
    }

    const auto logicalSource = sourceCatalog->getLogicalSource(statement.attachedTo);
    if (!logicalSource)
    {
        throw UnknownSourceName("{}", statement.attachedTo);
    }
    const auto created = sourceCatalog->addPhysicalSource(
        *logicalSource, physicalSourceConfig.type, host, physicalSourceConfig.sourceConfig, physicalSourceConfig.parserConfig);
    if (!created)
    {
        throw Exception(created.error());
    }
}

void createModel(
    const std::shared_ptr<ModelCatalog>& modelCatalog,
    const CreateModelStatement& statement,
    const std::filesystem::path& testDataDirectory)
{
    auto resolvedStatement = statement;
    auto path = std::filesystem::path(statement.path);
    if (!path.is_absolute())
    {
        path = testDataDirectory / path;
    }
    resolvedStatement.path = path.string();
    auto result = ModelStatementHandler(modelCatalog)(resolvedStatement);
    if (!result)
    {
        throw std::move(result).error();
    }
}

void prepareFixture(
    const FixtureStatement& fixture,
    const StatementBinder& binder,
    const std::shared_ptr<SourceCatalog>& sourceCatalog,
    const std::shared_ptr<ModelCatalog>& modelCatalog,
    SLTSinkFactory& sinkFactory,
    const std::shared_ptr<std::vector<std::jthread>>& sourceThreads,
    const SystestClusterConfiguration& cluster,
    const std::filesystem::path& workingDirectory,
    const std::filesystem::path& testDataDirectory,
    std::vector<std::filesystem::path>& generatedAttachments)
{
    const auto managedParser = AntlrSQLQueryParser::ManagedAntlrParser::create(fixture.sql);
    const auto parseResult = managedParser->parseSingle();
    if (!parseResult)
    {
        throw InvalidQuerySyntax("failed to to parse the query \"{}\"", replaceAll(fixture.sql, "\n", " "));
    }
    const auto binding = binder.bind(parseResult->get());
    if (!binding)
    {
        throw InvalidQuerySyntax("failed to to parse the query \"{}\"", replaceAll(fixture.sql, "\n", " "));
    }

    const auto& statement = *binding;
    if (const auto* logicalSource = std::get_if<CreateLogicalSourceStatement>(&statement))
    {
        createLogicalSource(sourceCatalog, *logicalSource);
    }
    else if (const auto* physicalSource = std::get_if<CreatePhysicalSourceStatement>(&statement))
    {
        createPhysicalSource(
            sourceCatalog,
            sourceThreads,
            *physicalSource,
            fixture.attachment,
            cluster,
            workingDirectory,
            testDataDirectory,
            generatedAttachments);
    }
    else if (const auto* sink = std::get_if<CreateSinkStatement>(&statement))
    {
        sinkFactory.registerSink(*sink);
    }
    else if (const auto* model = std::get_if<CreateModelStatement>(&statement))
    {
        createModel(modelCatalog, *model, testDataDirectory);
    }
    else
    {
        throw UnsupportedQuery();
    }
}

LogicalOperator updateAnonymousSource(
    const LogicalOperator& current, const std::filesystem::path& testDataDirectory, const SystestClusterConfiguration& cluster)
{
    std::vector<LogicalOperator> children;
    for (const auto& child : current.getChildren())
    {
        children.push_back(updateAnonymousSource(child, testDataDirectory, cluster));
    }

    if (const auto anonymousSource = current.tryGetAs<AnonymousSourceLogicalOperator>())
    {
        auto sourceConfig = anonymousSource.value()->getSourceConfig();
        auto parserConfig = anonymousSource.value()->getParserConfig();
        parserConfig.try_emplace(Identifier::parse("type"), "CSV");
        if (sourceConfig.contains(Identifier::parse("file_path")) && !sourceConfig.at(Identifier::parse("file_path")).starts_with("/"))
        {
            const auto filePath = testDataDirectory / sourceConfig.at(Identifier::parse("file_path"));
            sourceConfig.insert_or_assign(Identifier::parse("file_path"), filePath);
        }
        PRECONDITION(
            !cluster.allowSourcePlacement.empty(),
            "Topology must list at least one worker in allow_source_placement to assign a default anonymous source host");
        sourceConfig.try_emplace(Identifier::parse("host"), cluster.allowSourcePlacement.front().getRawValue());
        if (sourceConfig != anonymousSource.value()->getSourceConfig() || parserConfig != anonymousSource.value()->getParserConfig())
        {
            return AnonymousSourceLogicalOperator::create(
                       anonymousSource.value()->getSourceType(), anonymousSource.value()->getSourceSchema(), sourceConfig, parserConfig)
                .withChildrenUnsafe(children);
        }
    }
    return current.withChildrenUnsafe(std::move(children));
}

void setAnonymousSources(LogicalPlan& plan, const std::filesystem::path& testDataDirectory, const SystestClusterConfiguration& cluster)
{
    std::vector<LogicalOperator> roots;
    for (const auto& root : plan.getRootOperators())
    {
        roots.push_back(updateAnonymousSource(root, testDataDirectory, cluster));
    }
    plan = plan.withRootOperators(roots);
}

LogicalOperator setAnonymousSink(
    const TypedLogicalOperator<AnonymousSinkLogicalOperator>& sinkOperator,
    SLTSinkFactory& sinkFactory,
    const std::filesystem::path& outputFile)
{
    auto sinkConfig = sinkOperator->getSinkConfig();
    auto formatConfig = sinkOperator->getFormatConfig();
    sinkConfig.erase(Identifier::parse("file_path"));
    if (sinkOperator->getSinkType() == Identifier::parse("File") || sinkOperator->getSinkType() == Identifier::parse("CHECKSUM"))
    {
        sinkConfig.emplace(Identifier::parse("file_path"), outputFile);
    }
    if (!sinkConfig.contains(Identifier::parse("output_format")) && sinkOperator->getSinkType() != Identifier::parse("CHECKSUM")
        && sinkOperator->getSinkType() != Identifier::parse("VOID"))
    {
        sinkConfig.emplace(Identifier::parse("output_format"), "CSV");
    }
    if (sinkOperator->getSinkType() == Identifier::parse("CHECKSUM"))
    {
        formatConfig[Identifier::parse("quote_strings")] = "true";
    }
    const auto descriptor
        = sinkFactory.getAnonymousSink(sinkOperator->getTargetSchema(), sinkOperator->getSinkType(), sinkConfig, formatConfig);
    if (!descriptor)
    {
        throw InvalidConfigParameter("Failed to create anonymous sink of type {}", sinkOperator->getSinkType());
    }
    return SinkLogicalOperator::create(*descriptor).withChildrenUnsafe(sinkOperator->getChildren());
}

LogicalOperator setNamedSink(
    const TypedLogicalOperator<SinkLogicalOperator>& sinkOperator,
    SLTSinkFactory& sinkFactory,
    const TestCaseId& id,
    const std::string_view role,
    const std::filesystem::path& outputFile)
{
    const auto sinkName = sinkOperator->getSinkName();
    const auto assignedName
        = Identifier::parse(toUpperCase(fmt::format("{}_{}", sinkName.asCanonicalString(), executionIdentity(id, role))));
    auto descriptor = sinkFactory.createActualSink(sinkName, assignedName, outputFile);
    if (!descriptor)
    {
        throw descriptor.error();
    }
    return SinkLogicalOperator::create(*descriptor).withChildrenUnsafe(sinkOperator->getChildren());
}

void setSinks(
    LogicalPlan& plan,
    SLTSinkFactory& sinkFactory,
    const TestCaseId& id,
    const std::string_view role,
    const std::filesystem::path& outputFile,
    const bool anonymousOnly)
{
    std::vector<LogicalOperator> roots;
    for (const auto& root : plan.getRootOperators())
    {
        if (const auto anonymousSink = root.tryGetAs<AnonymousSinkLogicalOperator>())
        {
            roots.push_back(setAnonymousSink(*anonymousSink, sinkFactory, outputFile));
        }
        else if (const auto namedSink = root.tryGetAs<SinkLogicalOperator>())
        {
            roots.push_back(anonymousOnly ? root : setNamedSink(*namedSink, sinkFactory, id, role, outputFile));
        }
        else
        {
            throw UnsupportedQuery(
                "Invalid root operator \"{}\". Root operators must be SinkLogicalOperators or AnonymousSinkLogicalOperators.",
                root.getName());
        }
    }
    plan = plan.withRootOperators(roots);
}

PreparedStatement prepareStatement(std::string sql, DistributedLogicalPlan plan, std::filesystem::path outputFile)
{
    std::map<std::filesystem::path, uint64_t> metricOccurrences;
    for (const auto& source : getOperatorByType<SourceDescriptorLogicalOperator>(plan.getGlobalPlan()))
    {
        if (const auto path = source->getSourceDescriptor().tryGetFromConfig<std::string>("FILE_PATH"))
        {
            ++metricOccurrences[*path];
        }
        else
        {
            NES_INFO(
                "No file found for physical source {} for logical source {}",
                source->getSourceDescriptor().getPhysicalSourceId(),
                source->getSourceDescriptor().getLogicalSource().getLogicalSourceName());
        }
    }

    const auto sink = plan.getGlobalPlan().getRootOperators().at(0).tryGetAs<SinkLogicalOperator>();
    INVARIANT(sink.has_value(), "The optimized plan should have a sink operator");
    INVARIANT(sink.value()->getSinkDescriptor().has_value(), "The sink operator should have a sink descriptor");
    const auto descriptor = sink.value()->getSinkDescriptor().value();
    const auto sinkType = toUpperCase(descriptor.getSinkType());
    auto schema
        = sinkType == "CHECKSUM" ? SLTSinkFactory::checksumSchema : *get<std::shared_ptr<const ResultSchema>>(descriptor.getSchema());
    const auto output = sinkType == "VOID" ? OutputTarget{.kind = OutputTargetKind::Discard, .file = {}}
                                           : OutputTarget{.kind = OutputTargetKind::Table, .file = std::move(outputFile)};
    std::vector<PreparedSourceMetric> sourceMetrics;
    sourceMetrics.reserve(metricOccurrences.size());
    for (auto& [file, occurrences] : metricOccurrences)
    {
        sourceMetrics.push_back(PreparedSourceMetric{.file = std::move(file), .occurrences = occurrences});
    }
    return PreparedStatement{
        .sql = std::move(sql),
        .plan = std::move(plan),
        .outputSchema = std::move(schema),
        .output = output,
        .sourceMetrics = std::move(sourceMetrics)};
}

PreparedAction prepareAction(
    const ResolvedCase& testCase,
    const std::filesystem::path& workingDirectory,
    const std::filesystem::path& testDataDirectory,
    const QueryOptimizerConfiguration& optimizerConfiguration,
    const std::shared_ptr<SourceCatalog>& sourceCatalog,
    const std::shared_ptr<SinkCatalog>& sinkCatalog,
    const std::shared_ptr<ModelCatalog>& modelCatalog,
    const std::shared_ptr<WorkerCatalog>& workerCatalog,
    SLTSinkFactory& sinkFactory,
    const SystestClusterConfiguration& cluster)
{
    const auto primaryOutput = resultFile(workingDirectory, testCase.id, "primary");
    const QueryOptimizer optimizer{optimizerConfiguration, sourceCatalog, sinkCatalog, workerCatalog, modelCatalog};

    if (const auto* query = std::get_if<QueryAction>(&testCase.action))
    {
        if (query->kind == QueryKind::Explain)
        {
            const auto binder = StatementBinder{
                sourceCatalog, [](auto&& plan) { return AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(plan)>(plan)); }};
            const auto managedParser = AntlrSQLQueryParser::ManagedAntlrParser::create(query->sql);
            const auto parseResult = managedParser->parseSingle();
            if (!parseResult)
            {
                throw InvalidQuerySyntax("failed to parse the statement \"{}\"", replaceAll(query->sql, "\n", " "));
            }
            auto binding = binder.bind(parseResult->get());
            if (!binding)
            {
                throw InvalidQuerySyntax("failed to bind the statement \"{}\": {}", query->sql, binding.error());
            }
            auto* explain = std::get_if<ExplainQueryStatement>(&*binding);
            if (explain == nullptr)
            {
                throw UnsupportedQuery("expected an EXPLAIN statement, but got: \"{}\"", replaceAll(query->sql, "\n", " "));
            }
            setSinks(explain->plan, sinkFactory, testCase.id, "primary", primaryOutput, true);
            setAnonymousSources(explain->plan, testDataDirectory, cluster);
            return PreparedExplain{.sql = query->sql, .output = computeExplainOutput(*explain, optimizer)};
        }

        auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query->sql);
        setSinks(plan, sinkFactory, testCase.id, "primary", primaryOutput, false);
        plan.setQueryId(QueryId::createDistributed(DistributedQueryId(executionIdentity(testCase.id, "primary"))));
        setAnonymousSources(plan, testDataDirectory, cluster);
        return PreparedQuery{.statement = prepareStatement(query->sql, optimizer.optimize(plan), primaryOutput)};
    }

    const auto& differential = std::get<DifferentialAction>(testCase.action);
    const auto differentialOutput = resultFile(workingDirectory, testCase.id, "differential");
    auto primaryPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(differential.leftSql);
    auto differentialPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(differential.rightSql);
    setSinks(primaryPlan, sinkFactory, testCase.id, "primary", primaryOutput, false);
    setSinks(differentialPlan, sinkFactory, testCase.id, "differential", differentialOutput, false);
    setAnonymousSources(primaryPlan, testDataDirectory, cluster);
    setAnonymousSources(differentialPlan, testDataDirectory, cluster);
    primaryPlan.setQueryId(QueryId::createDistributed(DistributedQueryId(executionIdentity(testCase.id, "primary"))));
    differentialPlan.setQueryId(QueryId::createDistributed(DistributedQueryId(executionIdentity(testCase.id, "differential"))));
    return PreparedDifferential{
        .primary = prepareStatement(differential.leftSql, optimizer.optimize(primaryPlan), primaryOutput),
        .differential = prepareStatement(differential.rightSql, optimizer.optimize(differentialPlan), differentialOutput)};
}

PlanningFailure planningFailure(const Exception& exception)
{
    return PlanningFailure{.code = exception.code(), .message = exception.what()};
}

}

struct PreparedEnvironmentContext::Impl
{
    std::shared_ptr<SourceCatalog> sourceCatalog = std::make_shared<SourceCatalog>();
    std::shared_ptr<SinkCatalog> sinkCatalog = std::make_shared<SinkCatalog>();
    std::shared_ptr<ModelCatalog> modelCatalog = std::make_shared<ModelCatalog>();
    std::shared_ptr<std::vector<std::jthread>> sourceThreads = std::make_shared<std::vector<std::jthread>>();
    std::vector<std::filesystem::path> generatedAttachments;
    std::vector<FixtureStatement> fixtures;
    size_t nextFixture = 0;
    std::shared_ptr<WorkerCatalog> workerCatalog;
    std::unique_ptr<SLTSinkFactory> sinkFactory;
    SystestClusterConfiguration cluster;
};

PreparedExecutionCatalog::PreparedExecutionCatalog(std::map<TestCaseId, PreparedExecution> executions) : executions(std::move(executions))
{
}

const PreparedExecution& PreparedExecutionCatalog::at(const TestCaseId& id) const
{
    return executions.at(id);
}

const PreparedExecution* PreparedExecutionCatalog::find(const TestCaseId& id) const
{
    const auto found = executions.find(id);
    return found == executions.end() ? nullptr : &found->second;
}

std::vector<TestCaseId> PreparedExecutionCatalog::ids() const
{
    return executions | std::views::keys | std::ranges::to<std::vector>();
}

PreparedEnvironmentContext::PreparedEnvironmentContext(std::shared_ptr<Impl> impl) : impl(std::move(impl))
{
}

PreparedEnvironmentContext::~PreparedEnvironmentContext() = default;

std::shared_ptr<const SourceCatalog> PreparedEnvironmentContext::sourceCatalog() const
{
    return impl->sourceCatalog;
}

std::shared_ptr<const SinkCatalog> PreparedEnvironmentContext::sinkCatalog() const
{
    return impl->sinkCatalog;
}

std::shared_ptr<const ModelCatalog> PreparedEnvironmentContext::modelCatalog() const
{
    return impl->modelCatalog;
}

const std::vector<std::filesystem::path>& PreparedEnvironmentContext::generatedAttachments() const
{
    return impl->generatedAttachments;
}

size_t PreparedEnvironmentContext::sourceThreadCount() const
{
    return impl->sourceThreads->size();
}

PreparedEnvironmentCatalog::PreparedEnvironmentCatalog(std::map<EnvironmentId, PreparedEnvironment> environments)
    : environments(std::move(environments))
{
}

const PreparedEnvironment& PreparedEnvironmentCatalog::at(const EnvironmentId id) const
{
    return environments.at(id);
}

const PreparedEnvironment* PreparedEnvironmentCatalog::find(const EnvironmentId id) const
{
    const auto found = environments.find(id);
    return found == environments.end() ? nullptr : &found->second;
}

std::vector<EnvironmentId> PreparedEnvironmentCatalog::ids() const
{
    return environments | std::views::keys | std::ranges::to<std::vector>();
}

std::expected<std::shared_ptr<PreparedEnvironmentCatalog>, Exception> EnvironmentPreparer::prepare(const ResolvedRun& run) const
try
{
    std::map<std::filesystem::path, std::shared_ptr<PreparedEnvironmentContext>> contexts;
    std::map<EnvironmentId, PreparedEnvironment> preparedEnvironments;
    for (const auto& environment : run.environments)
    {
        auto context = contexts.find(environment.relativeTestFile);
        if (context == contexts.end())
        {
            auto resources = std::make_shared<PreparedEnvironmentContext::Impl>();
            resources->cluster = environment.cluster;
            resources->workerCatalog = std::make_shared<WorkerCatalog>();
            for (const auto& [host, data, capacity, downstream, config] : environment.cluster.workers)
            {
                resources->workerCatalog->addWorker(host, data, capacity, downstream, config);
            }
            resources->sinkFactory = std::make_unique<SLTSinkFactory>(resources->sinkCatalog, environment.cluster.allowSinkPlacement);
            resources->fixtures = environment.setupStatements;
            auto preparedContext = std::shared_ptr<PreparedEnvironmentContext>(new PreparedEnvironmentContext(std::move(resources)));
            context = contexts.emplace(environment.relativeTestFile, std::move(preparedContext)).first;
        }
        preparedEnvironments.emplace(environment.id, PreparedEnvironment{.id = environment.id, .context = context->second});
    }
    return std::make_shared<PreparedEnvironmentCatalog>(std::move(preparedEnvironments));
}
catch (const Exception& exception)
{
    return std::unexpected(exception);
}
catch (const std::exception& exception)
{
    return std::unexpected(TestException("{}", exception.what()));
}

ExecutionPreparer::ExecutionPreparer(
    std::filesystem::path workingDirectory,
    std::filesystem::path testDataDirectory,
    QueryOptimizerConfiguration queryOptimizerConfiguration)
    : workingDirectory(std::move(workingDirectory))
    , testDataDirectory(std::move(testDataDirectory))
    , queryOptimizerConfiguration(std::move(queryOptimizerConfiguration))
{
}

std::expected<std::shared_ptr<const PreparedExecutionCatalog>, Exception>
ExecutionPreparer::prepare(const ResolvedRun& run, PreparedEnvironmentCatalog& environments) const
try
{
    const auto prepareFixturesUntil
        = [&](const std::shared_ptr<PreparedEnvironmentContext>& context, const std::optional<size_t> beforeLine)
    {
        auto& resources = *context->impl;
        const auto binder = StatementBinder{
            resources.sourceCatalog,
            [](auto&& plan) { return AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(plan)>(plan)); }};
        while (resources.nextFixture < resources.fixtures.size()
               && (!beforeLine || resources.fixtures[resources.nextFixture].source.lastLine < *beforeLine))
        {
            prepareFixture(
                resources.fixtures[resources.nextFixture],
                binder,
                resources.sourceCatalog,
                resources.modelCatalog,
                *resources.sinkFactory,
                resources.sourceThreads,
                resources.cluster,
                workingDirectory,
                testDataDirectory,
                resources.generatedAttachments);
            ++resources.nextFixture;
        }
    };

    std::map<TestCaseId, std::variant<std::shared_ptr<const PreparedAction>, PlanningFailure>> preparedByCase;
    std::map<TestCaseId, PreparedExecution> executions;
    for (const auto& testCase : run.cases)
    {
        const auto& environment = run.environment(testCase->environment);
        const auto& context = environments.at(testCase->environment).context;
        prepareFixturesUntil(context, testCase->source.firstLine);
        auto prepared = preparedByCase.find(testCase->id);
        if (prepared == preparedByCase.end())
        {
            try
            {
                auto action = std::make_shared<const PreparedAction>(prepareAction(
                    *testCase,
                    workingDirectory,
                    testDataDirectory,
                    queryOptimizerConfiguration,
                    context->impl->sourceCatalog,
                    context->impl->sinkCatalog,
                    context->impl->modelCatalog,
                    context->impl->workerCatalog,
                    *context->impl->sinkFactory,
                    environment.cluster));
                prepared = preparedByCase.emplace(testCase->id, std::move(action)).first;
            }
            catch (const Exception& exception)
            {
                prepared = preparedByCase.emplace(testCase->id, planningFailure(exception)).first;
            }
        }
        executions.emplace(testCase->id, PreparedExecution{.environment = testCase->environment, .prepared = prepared->second});
    }

    std::set<const PreparedEnvironmentContext*> finalizedContexts;
    for (const auto& environment : run.environments)
    {
        const auto& context = environments.at(environment.id).context;
        if (finalizedContexts.insert(context.get()).second)
        {
            prepareFixturesUntil(context, std::nullopt);
        }
    }
    return std::make_shared<const PreparedExecutionCatalog>(std::move(executions));
}
catch (const Exception& exception)
{
    return std::unexpected(exception);
}
catch (const std::exception& exception)
{
    return std::unexpected(TestException("{}", exception.what()));
}

std::expected<PreparedRun, Exception> prepareSystestRun(
    ResolvedRun run,
    const std::filesystem::path& workingDirectory,
    const std::filesystem::path& testDataDirectory,
    const QueryOptimizerConfiguration& queryOptimizerConfiguration)
{
    auto environments = EnvironmentPreparer{}.prepare(run);
    if (!environments)
    {
        return std::unexpected(environments.error());
    }
    auto executions = ExecutionPreparer(workingDirectory, testDataDirectory, queryOptimizerConfiguration).prepare(run, **environments);
    if (!executions)
    {
        return std::unexpected(executions.error());
    }
    return PreparedRun{.resolved = std::move(run), .environments = std::move(*environments), .executions = std::move(*executions)};
}

}
