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

#include <SQLQueryParser/StatementBinder.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <Util/Strings.hpp>

#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataTypeProvider.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Overloaded.hpp>
#include <boost/url/parse.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <BailErrorStrategy.h>
#include <CommonTokenStream.h>
#include <Exceptions.h>
#include <DataTypes/DataType.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <ErrorHandling.hpp>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <CommonParserFunctions.hpp>

namespace NES
{

namespace
{
/// A worker address is `host:port`.
/// A URI parser would read the host as the scheme, and rejects an IPv4 address, because a scheme starts with a letter.
std::string bindWorkerAddress(const std::string& address)
{
    if (const auto authority = boost::urls::parse_authority(address);
        not authority or authority->has_userinfo() or authority->host().empty() or not authority->has_port())
    {
        throw InvalidQuerySyntax("'{}' is not a worker address of the form host:port", address);
    }
    return address;
}
}

/// NOLINTBEGIN(readability-convert-member-functions-to-static)
class StatementBinder::Impl
{
    std::function<LogicalPlan(AntlrSQLParser::QueryContext*)> queryBinder;

public:
    explicit Impl(const std::function<LogicalPlan(AntlrSQLParser::QueryContext*)>& queryBinder) : queryBinder(queryBinder) { }

    ~Impl() = default;

    CreateLogicalSourceStatement
    bindCreateLogicalSourceStatement(AntlrSQLParser::CreateLogicalSourceDefinitionContext* logicalSourceDefAST) const
    {
        const auto sourceName = bindIdentifier(logicalSourceDefAST->sourceName->strictIdentifier());
        const auto schema = bindSchema(logicalSourceDefAST->schemaDefinition());
        return CreateLogicalSourceStatement{
            .name = sourceName, .schema = schema, .ifNotExists = logicalSourceDefAST->ifNotExists() != nullptr};
    }

    CreatePhysicalSourceStatement
    bindCreatePhysicalSourceStatement(AntlrSQLParser::CreatePhysicalSourceDefinitionContext* physicalSourceDefAST) const
    {
        const auto logicalSourceName = LogicalSourceName(bindIdentifier(physicalSourceDefAST->logicalSource->strictIdentifier()));
        /// TODO #764 use normal identifiers for types
        const Identifier type = bindIdentifier(physicalSourceDefAST->type);
        const auto configOptions = [&]()
        {
            if (physicalSourceDefAST->optionsClause() != nullptr)
            {
                return bindConfigOptions(physicalSourceDefAST->optionsClause()->options->namedConfigExpression());
            }
            return ConfigMap{};
        }();

        const auto parserConfig = parseInputFormatterConfig(configOptions);
        auto sourceConfig = getSourceConfig(configOptions);

        /// "host" determines worker placement, not source behavior — extract it from the config map into a dedicated field.
        std::optional<Host> host;
        if (auto it = sourceConfig.find(Identifier::parse("host")); it != sourceConfig.end())
        {
            host = Host(it->second);
            sourceConfig.erase(it);
        }

        return CreatePhysicalSourceStatement{
            .attachedTo = logicalSourceName,
            .sourceType = type,
            .host = host,
            .sourceConfig = sourceConfig,
            .parserConfig = parserConfig,
            .ifNotExists = physicalSourceDefAST->ifNotExists() != nullptr};
    }

    /// MAX_OPERATORS, DATA and DOWNSTREAM make up the worker's catalog entry: its placement capacity, its data channel, and its links in the topology.
    /// Every other option is the worker's own configuration, stored under a dotted key.
    /// Example: `'4' AS "worker"."query_engine"."number_of_worker_threads"` gives `worker.query_engine.number_of_worker_threads -> 4`.
    CreateWorkerStatement bindCreateWorkerStatement(AntlrSQLParser::CreateWorkerDefinitionContext* workerDefAST) const
    {
        static const auto MaxOperators = Identifier::parse("MAX_OPERATORS");
        static const auto Data = Identifier::parse("DATA");
        static const auto Downstream = Identifier::parse("DOWNSTREAM");

        const auto options = (workerDefAST->optionsClause() != nullptr)
            ? bindConfigOptionsWithDuplicates(workerDefAST->optionsClause()->options->namedConfigExpression())
            : ConfigMultiMap{};

        auto worker = CreateWorkerStatement{
            .host = bindWorkerAddress(bindStringLiteral(workerDefAST->hostaddr)),
            .dataAddress = {},
            .maxOperators = std::nullopt,
            .downstream = {},
            .config = {},
            .ifNotExists = workerDefAST->ifNotExists() != nullptr};
        for (const auto& [key, value] : options)
        {
            const auto* literal = std::get_if<Literal>(&value);
            if (const auto name = key.size() == 1 ? std::optional{*std::ranges::begin(key)} : std::nullopt; name == MaxOperators)
            {
                if (literal == nullptr or not std::holds_alternative<uint64_t>(*literal))
                {
                    throw InvalidQuerySyntax("MAX_OPERATORS must be an unsigned integer literal");
                }
                worker.maxOperators = std::get<uint64_t>(*literal);
            }
            else if (name == Data or name == Downstream)
            {
                if (literal == nullptr or not std::holds_alternative<std::string>(*literal))
                {
                    throw InvalidQuerySyntax("{} must be a string literal", *name);
                }
                auto address = bindWorkerAddress(std::get<std::string>(*literal));
                if (name == Data)
                {
                    worker.dataAddress = std::move(address);
                }
                else
                {
                    worker.downstream.push_back(std::move(address));
                }
            }
            else
            {
                if (literal == nullptr)
                {
                    throw InvalidQuerySyntax("A worker configuration value must be a literal");
                }
                const auto path = key | std::views::transform([](const Identifier& part) { return part.asCanonicalString(); });
                worker.config.insert_or_assign(fmt::format("{}", fmt::join(path, ".")), literalToString(*literal));
            }
        }
        if (worker.dataAddress.empty())
        {
            throw InvalidQuerySyntax("CREATE WORKER needs the address of its data channel, e.g. SET ('host:9090' AS DATA)");
        }
        return worker;
    }

    CreateSinkStatement bindCreateSinkStatement(AntlrSQLParser::CreateSinkDefinitionContext* sinkDefAST) const
    {
        const auto sinkName = bindIdentifier(sinkDefAST->sinkName->strictIdentifier());
        /// A name of only digits is reserved, because an anonymous sink is identified by its numeric id.
        if (std::ranges::all_of(sinkName.asCanonicalString(), [](const unsigned char character) { return std::isdigit(character) != 0; }))
        {
            throw InvalidQuerySyntax("Sink name '{}' is invalid: only-digit names are reserved", sinkName);
        }
        const Identifier sinkType = bindIdentifier(sinkDefAST->type);
        const auto configOptions = [&]()
        {
            if (sinkDefAST->optionsClause() != nullptr)
            {
                return bindConfigOptions(sinkDefAST->optionsClause()->options->namedConfigExpression());
            }
            return ConfigMap{};
        }();
        std::unordered_map<Identifier, std::string> sinkOptions{};
        static const auto SinkIdentifier = Identifier::parse("SINK");
        if (const auto sinkConfigIter = configOptions.find(SinkIdentifier); sinkConfigIter != configOptions.end())
        {
            sinkOptions = sinkConfigIter->second
                | std::views::filter([](auto& pair) { return std::holds_alternative<Literal>(pair.second); })
                | std::views::transform([](auto& pair)
                                        { return std::make_pair(pair.first, literalToString(std::get<Literal>(pair.second))); })
                | std::ranges::to<std::unordered_map<Identifier, std::string>>();
        }
        std::unordered_map<Identifier, std::string> formatOptions{};
        if (const auto formatConfigIter = configOptions.find(Identifier::parse(std::string{OUTPUT_FORMATTER_OPTION_GROUP}));
            formatConfigIter != configOptions.end())
        {
            formatOptions = formatConfigIter->second
                | std::views::filter([](auto& pair) { return std::holds_alternative<Literal>(pair.second); })
                | std::views::transform([](auto& pair)
                                        { return std::make_pair(pair.first, literalToString(std::get<Literal>(pair.second))); })
                | std::ranges::to<std::unordered_map<Identifier, std::string>>();
        }
        /// "host" determines worker placement, not sink behavior — extract it from the config map into a dedicated field.
        std::optional<Host> host;
        if (auto it = sinkOptions.find(Identifier::parse("host")); it != sinkOptions.end())
        {
            host = Host(it->second);
            sinkOptions.erase(it);
        }

        const auto schema = bindSchema(sinkDefAST->schemaDefinition());
        return CreateSinkStatement{
            .name = sinkName,
            .sinkType = sinkType,
            .schema = schema,
            .host = host,
            .sinkConfig = sinkOptions,
            .formatConfig = formatOptions,
            .ifNotExists = sinkDefAST->ifNotExists() != nullptr};
    }

    CreateModelStatement bindCreateModelStatement(AntlrSQLParser::CreateModelDefinitionContext* modelDefAST) const
    {
        const auto modelName = bindIdentifier(modelDefAST->modelName->strictIdentifier());
        const auto modelPath = bindStringLiteral(modelDefAST->modelPath);
        std::vector<UnqualifiedUnboundField> inputs;
        for (auto* const inputField : modelDefAST->modelInputField())
        {
            inputs.emplace_back(
                bindIdentifier(inputField->identifier()), bindDataType(inputField->typeDefinition(), DataType::NULLABLE::NOT_NULLABLE));
        }

        std::vector<UnqualifiedUnboundField> outputs;
        for (auto* const outputField : modelDefAST->modelOutputField())
        {
            outputs.emplace_back(
                bindIdentifier(outputField->identifier()), bindDataType(outputField->typeDefinition(), DataType::NULLABLE::NOT_NULLABLE));
        }
        auto inputSchemaExp = Schema<UnqualifiedUnboundField, Ordered>::tryCreateCollisionFree(std::move(inputs))
                                  .transform_error(Schema<UnqualifiedUnboundField, Ordered>::createCollisionString);
        auto outputSchemaExp = Schema<UnqualifiedUnboundField, Ordered>::tryCreateCollisionFree(std::move(outputs))
                                   .transform_error(Schema<UnqualifiedUnboundField, Ordered>::createCollisionString);

        if (!inputSchemaExp.has_value())
        {
            throw FieldAlreadyExists("Field name collision in model input schema {}", inputSchemaExp.error());
        }
        if (!outputSchemaExp.has_value())
        {
            throw FieldAlreadyExists("Field name collision in model output schema {}", outputSchemaExp.error());
        }

        return CreateModelStatement{
            .name = fmt::format("{}", modelName),
            .path = modelPath,
            .inputs = std::move(inputSchemaExp).value(),
            .outputs = std::move(outputSchemaExp).value(),
            .ifNotExists = modelDefAST->ifNotExists() != nullptr};
    }

    Statement bindCreateStatement(AntlrSQLParser::CreateStatementContext* createAST) const
    {
        if (auto* const logicalSourceDefAST = createAST->createDefinition()->createLogicalSourceDefinition();
            logicalSourceDefAST != nullptr)
        {
            return bindCreateLogicalSourceStatement(logicalSourceDefAST);
        }
        if (auto* const physicalSourceDefAST = createAST->createDefinition()->createPhysicalSourceDefinition();
            physicalSourceDefAST != nullptr)
        {
            return bindCreatePhysicalSourceStatement(physicalSourceDefAST);
        }
        if (auto* const sinkDefAST = createAST->createDefinition()->createSinkDefinition(); sinkDefAST != nullptr)
        {
            return bindCreateSinkStatement(sinkDefAST);
        }
        if (auto* const workerDefAST = createAST->createDefinition()->createWorkerDefinition(); workerDefAST != nullptr)
        {
            return bindCreateWorkerStatement(workerDefAST);
        }
        if (auto* const modelDefAST = createAST->createDefinition()->createModelDefinition(); modelDefAST != nullptr)
        {
            return bindCreateModelStatement(modelDefAST);
        }
        if (auto* const queryDefAST = createAST->createDefinition()->createQueryDefinition(); queryDefAST != nullptr)
        {
            return QueryStatement{
                .plan = queryBinder(queryDefAST->query()),
                .name = bindIdentifier(queryDefAST->queryName->strictIdentifier()),
                .ifNotExists = queryDefAST->ifNotExists() != nullptr};
        }
        throw InvalidStatement("Unrecognized CREATE statement");
    }

    ShowLogicalSourcesStatement bindShowLogicalSourcesStatement(const AntlrSQLParser::ShowFilterContext* showFilter) const
    {
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            static const auto NameIdentifier = Identifier::parse("NAME");
            if (attr != NameIdentifier)
            {
                throw InvalidQuerySyntax("Filter for SHOW LOGICAL SOURCES must be on name attribute");
            }
            if (not std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW LOGICAL SOURCES must be a string");
            }
            return ShowLogicalSourcesStatement{.name = bindIdentifier(std::get<std::string>(value))};
        }
        return ShowLogicalSourcesStatement{.name = std::nullopt};
    }

    ShowPhysicalSourcesStatement bindShowPhysicalSourcesStatement(
        const AntlrSQLParser::ShowFilterContext* showFilter,
        const AntlrSQLParser::ShowPhysicalSourcesSubjectContext* physicalSourcesSubject) const
    {
        std::optional<LogicalSourceName> logicalSourceName{};
        if (physicalSourcesSubject->logicalSourceName != nullptr)
        {
            logicalSourceName = LogicalSourceName(bindIdentifier(physicalSourcesSubject->logicalSourceName));
        }
        if (showFilter != nullptr)
        {
            return ShowPhysicalSourcesStatement{
                .logicalSource = logicalSourceName, .id = requireId<PhysicalSourceId>(bindShowFilter(showFilter), "SHOW PHYSICAL SOURCES")};
        }
        return ShowPhysicalSourcesStatement{.logicalSource = logicalSourceName, .id = std::nullopt};
    }

    ShowSinksStatement bindShowSinksStatement(const AntlrSQLParser::ShowFilterContext* showFilter) const
    {
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            static const auto NameIdentifier = Identifier::parse("NAME");
            if (attr != NameIdentifier)
            {
                throw InvalidQuerySyntax("Filter for SHOW SINKS must be on name attribute");
            }
            if (not std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW SINKS must be a string");
            }
            return ShowSinksStatement{.name = bindIdentifier(std::get<std::string>(value))};
        }
        return ShowSinksStatement{.name = std::nullopt};
    }

    ShowModelsStatement bindShowModelsStatement(const AntlrSQLParser::ShowFilterContext* showFilter) const
    {
        if (showFilter == nullptr)
        {
            return ShowModelsStatement{.name = std::nullopt};
        }
        const auto filter = bindShowFilter(showFilter);
        return ShowModelsStatement{.name = bindIdentifier(requireFilterValue<std::string>(filter, "NAME", "a string", "SHOW MODELS"))};
    }

    ShowQueriesStatement bindShowQueriesStatement(const AntlrSQLParser::ShowFilterContext* showFilter) const
    {
        if (showFilter == nullptr)
        {
            return ShowQueriesStatement{.id = std::nullopt, .name = std::nullopt};
        }

        const auto filter = bindShowFilter(showFilter);
        static const auto IdIdentifier = Identifier::parse("ID");
        static const auto NameIdentifier = Identifier::parse("NAME");
        if (filter.first == IdIdentifier)
        {
            return ShowQueriesStatement{.id = requireId<QueryId>(filter, "SHOW QUERIES"), .name = std::nullopt};
        }
        if (filter.first == NameIdentifier)
        {
            return ShowQueriesStatement{
                .id = std::nullopt, .name = Identifier::parse(requireFilterValue<std::string>(filter, "NAME", "a string", "SHOW QUERIES"))};
        }
        throw InvalidQuerySyntax("Filter for SHOW QUERIES must be on ID or NAME attribute");
    }

    static ShowWorkersStatement bindShowWorkersStatement(const AntlrSQLParser::ShowFilterContext* showFilter)
    {
        if (showFilter == nullptr)
        {
            return ShowWorkersStatement{.host = std::nullopt};
        }
        const auto filter = bindShowFilter(showFilter);
        return ShowWorkersStatement{.host = bindWorkerAddress(requireFilterValue<std::string>(filter, "HOST", "a string", "SHOW WORKERS"))};
    }

    Statement bindShowStatement(AntlrSQLParser::ShowStatementContext* showAST) const
    {
        auto* showFilter = showAST->showFilter();

        if (const auto* logicalSourcesSubject = dynamic_cast<AntlrSQLParser::ShowLogicalSourcesSubjectContext*>(showAST->showSubject());
            logicalSourcesSubject != nullptr)
        {
            return bindShowLogicalSourcesStatement(showFilter);
        }
        if (auto* physicalSourcesSubject = dynamic_cast<AntlrSQLParser::ShowPhysicalSourcesSubjectContext*>(showAST->showSubject());
            physicalSourcesSubject != nullptr)
        {
            return bindShowPhysicalSourcesStatement(showFilter, physicalSourcesSubject);
        }
        if (const auto* queriesSubject = dynamic_cast<AntlrSQLParser::ShowQueriesSubjectContext*>(showAST->showSubject());
            queriesSubject != nullptr)
        {
            return bindShowQueriesStatement(showFilter);
        }
        if (const auto* sinksSubject = dynamic_cast<AntlrSQLParser::ShowSinksSubjectContext*>(showAST->showSubject());
            sinksSubject != nullptr)
        {
            return bindShowSinksStatement(showFilter);
        }
        if (const auto* modelsSubject = dynamic_cast<AntlrSQLParser::ShowModelsSubjectContext*>(showAST->showSubject());
            modelsSubject != nullptr)
        {
            return bindShowModelsStatement(showFilter);
        }
        if (const auto* workersSubject = dynamic_cast<AntlrSQLParser::ShowWorkersSubjectContext*>(showAST->showSubject());
            workersSubject != nullptr)
        {
            return bindShowWorkersStatement(showFilter);
        }
        if (const auto* versionSubject = dynamic_cast<AntlrSQLParser::ShowVersionSubjectContext*>(showAST->showSubject());
            versionSubject != nullptr)
        {
            if (showFilter == nullptr)
            {
                return ShowVersionStatement{.host = std::nullopt};
            }
            const auto filter = bindShowFilter(showFilter);
            return ShowVersionStatement{
                .host = bindWorkerAddress(requireFilterValue<std::string>(filter, "HOST", "a string", "SHOW VERSION"))};
        }
        throw InvalidStatement("Unrecognized SHOW statement");
    }

    /// Validate a filter and extract its typed value, or throw a syntax error tied to `subject`.
    template <typename T>
    static T requireFilterValue(
        const std::pair<Identifier, Literal>& filter,
        std::string_view expectedAttr,
        std::string_view expectedTypeDescription,
        std::string_view subject)
    {
        if (filter.first != bindIdentifier(std::string{expectedAttr}))
        {
            throw InvalidQuerySyntax("Filter for {} must be on {} attribute", subject, expectedAttr);
        }
        if (!std::holds_alternative<T>(filter.second))
        {
            throw InvalidQuerySyntax("Filter value for {} must be {}", subject, expectedTypeDescription);
        }
        return std::get<T>(filter.second);
    }

    template <typename Id>
    static Id requireId(const std::pair<Identifier, Literal>& filter, std::string_view subject)
    {
        const auto value = requireFilterValue<uint64_t>(filter, "ID", "an unsigned integer", subject);
        if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
        {
            throw InvalidQuerySyntax("Filter value for {} must be at most {}", subject, std::numeric_limits<int64_t>::max());
        }
        return Id{static_cast<int64_t>(value)};
    }

    using DropFilter = std::pair<Identifier, Literal>;

    static Selection<DropFilter> bindDropSelection(AntlrSQLParser::DropSelectionContext* selectionAst)
    {
        if (selectionAst->ALL() != nullptr)
        {
            return Selection<DropFilter>::all();
        }
        return Selection<DropFilter>::of(bindDropFilter(selectionAst->dropFilter()));
    }

    static DropLogicalSourceStatement bindDropLogicalSource(const Selection<DropFilter>& selection)
    {
        return DropLogicalSourceStatement{
            .source = selection.transform(
                [](const DropFilter& filter)
                {
                    return LogicalSourceName{
                        Identifier::parse(requireFilterValue<std::string>(filter, "NAME", "a string", "DROP LOGICAL SOURCE"))};
                })};
    }

    [[nodiscard]] static DropPhysicalSourceStatement bindDropPhysicalSource(const Selection<DropFilter>& selection)
    {
        return DropPhysicalSourceStatement{
            .id
            = selection.transform([](const DropFilter& filter) { return requireId<PhysicalSourceId>(filter, "DROP PHYSICAL SOURCE"); })};
    }

    /// DROP QUERY and STOP QUERY select their queries the same way.
    template <typename QueryStatement>
    static QueryStatement bindQuerySelection(const Selection<DropFilter>& selection, const std::string_view statementName)
    {
        return QueryStatement{
            .queries = selection.transform(
                [statementName](const DropFilter& filter) -> std::variant<QueryId, Identifier>
                {
                    static const auto IdIdentifier = Identifier::parse("ID");
                    static const auto NameIdentifier = Identifier::parse("NAME");
                    if (filter.first == IdIdentifier)
                    {
                        return requireId<QueryId>(filter, statementName);
                    }
                    if (filter.first == NameIdentifier)
                    {
                        return Identifier::parse(requireFilterValue<std::string>(filter, "NAME", "a string", statementName));
                    }
                    throw InvalidQuerySyntax("Filter for {} must be on ID or NAME attribute", statementName);
                })};
    }

    static StopQueryStatement bindStopStatement(AntlrSQLParser::StopStatementContext* stopAst)
    {
        return bindQuerySelection<StopQueryStatement>(bindDropSelection(stopAst->dropSelection()), "STOP QUERY");
    }

    static DropSinkStatement bindDropSink(const Selection<DropFilter>& selection)
    {
        return DropSinkStatement{
            .name
            = selection.transform([](const DropFilter& filter)
                                  { return Identifier::parse(requireFilterValue<std::string>(filter, "NAME", "a string", "DROP SINK")); })};
    }

    static DropModelStatement bindDropModel(const Selection<DropFilter>& selection)
    {
        return DropModelStatement{
            .name = selection.transform([](const DropFilter& filter)
                                        { return requireFilterValue<std::string>(filter, "NAME", "a string", "DROP MODEL"); })};
    }

    static DropWorkerStatement bindDropWorker(const Selection<DropFilter>& selection)
    {
        if (const auto& filter = selection.getTarget(); filter.has_value())
        {
            return DropWorkerStatement{
                .host = bindWorkerAddress(requireFilterValue<std::string>(*filter, "HOST", "a string", "DROP WORKER"))};
        }
        throw InvalidQuerySyntax("DROP WORKER requires a WHERE filter on the HOST attribute");
    }

    Statement bindDropStatement(AntlrSQLParser::DropStatementContext* dropAst) const
    {
        const auto selection = bindDropSelection(dropAst->dropSelection());
        auto* const subject = dropAst->dropSubject();

        if (subject->dropQuery() != nullptr)
        {
            return bindQuerySelection<DropQueryStatement>(selection, "DROP QUERY");
        }
        if (auto* const dropSourceAst = subject->dropSource())
        {
            if (dropSourceAst->dropLogicalSourceSubject() != nullptr)
            {
                return bindDropLogicalSource(selection);
            }
            if (dropSourceAst->dropPhysicalSourceSubject() != nullptr)
            {
                return bindDropPhysicalSource(selection);
            }
        }
        if (subject->dropSink() != nullptr)
        {
            return bindDropSink(selection);
        }
        if (subject->dropModel() != nullptr)
        {
            return bindDropModel(selection);
        }
        if (subject->dropWorker() != nullptr)
        {
            return bindDropWorker(selection);
        }
        throw InvalidStatement("Unrecognized DROP statement");
    }

    std::unordered_set<ExplainStage> bindExplainStages(AntlrSQLParser::ExplainStagesContext* stagesContext) const
    {
        std::unordered_set<ExplainStage> explainStages;

        for (auto* const stageContext : stagesContext->explainStage())
        {
            if (stageContext->LOGICAL() != nullptr)
            {
                if (!explainStages.insert(ExplainStage::Logical).second)
                {
                    throw InvalidStatement("Duplicate EXPLAIN stage: LOGICAL");
                }
            }
            else
            {
                const auto identifier = bindIdentifier(stageContext->identifier());

                if (identifier == Identifier::parse("logical"))
                {
                    if (!explainStages.insert(ExplainStage::Logical).second)
                    {
                        throw InvalidStatement("Duplicate EXPLAIN stage: LOGICAL");
                    }
                }
                else if (identifier == Identifier::parse("optimized"))
                {
                    if (!explainStages.insert(ExplainStage::Optimized).second)
                    {
                        throw InvalidStatement("Duplicate EXPLAIN stage: OPTIMIZED");
                    }
                }
                else if (identifier == Identifier::parse("distributed"))
                {
                    if (!explainStages.insert(ExplainStage::Distributed).second)
                    {
                        throw InvalidStatement("Duplicate EXPLAIN stage: DISTRIBUTED");
                    }
                }
                else
                {
                    throw InvalidStatement("Unrecognized EXPLAIN stage: {}", identifier);
                }
            }
        }

        return explainStages;
    }

    ExplainFormat bindExplainFormat(AntlrSQLParser::ExplainFormatContext* formatAST) const
    {
        if (formatAST->TEXT() != nullptr)
        {
            return ExplainFormat::Text;
        }

        auto parsedFormatIdentifier = bindIdentifier(formatAST->identifier());

        if (parsedFormatIdentifier == Identifier::parse("text"))
        {
            return ExplainFormat::Text;
        }
        if (parsedFormatIdentifier == Identifier::parse("verbose"))
        {
            return ExplainFormat::Verbose;
        }
        if (parsedFormatIdentifier == Identifier::parse("visual"))
        {
            return ExplainFormat::Visual;
        }
        throw InvalidStatement("Unrecognized EXPLAIN format: {}", parsedFormatIdentifier);
    }

    Statement bindExplainStatement(AntlrSQLParser::ExplainStatementContext* explainAst) const
    {
        INVARIANT(explainAst->query() != nullptr, "Should be enforced by antlr");
        ExplainFormat format = ExplainFormat::Visual;
        std::unordered_set<ExplainStage> stages = {ExplainStage::Logical, ExplainStage::Optimized, ExplainStage::Distributed};
        if (auto* const stagesAST = explainAst->explainStages())
        {
            stages = bindExplainStages(stagesAST);
        }
        if (auto* const formatAST = explainAst->explainFormat())
        {
            format = bindExplainFormat(formatAST);
        }

        return ExplainQueryStatement{.plan = queryBinder(explainAst->query()), .explainFormat = format, .explainStages = stages};
    }

    std::expected<Statement, Exception> bind(AntlrSQLParser::StatementContext* statementAST) const
    {
        try
        {
            if (auto* const createAST = statementAST->createStatement(); createAST != nullptr)
            {
                return bindCreateStatement(createAST);
            }
            if (auto* showAST = statementAST->showStatement(); showAST != nullptr)
            {
                return bindShowStatement(showAST);
            }
            if (auto* dropAst = statementAST->dropStatement(); dropAst != nullptr)
            {
                return bindDropStatement(dropAst);
            }
            if (auto* const stopAst = statementAST->stopStatement())
            {
                return bindStopStatement(stopAst);
            }
            if (auto* const explainStatementAST = statementAST->explainStatement())
            {
                return bindExplainStatement(explainStatementAST);
            }
            if (auto* const queryAst = statementAST->query(); queryAst != nullptr)
            {
                return QueryStatement{.plan = queryBinder(queryAst), .name = std::nullopt};
            }

            throw InvalidStatement(statementAST->toString());
        }
        catch (Exception& e)
        {
            return std::unexpected{e};
        }
        catch (const std::exception& e)
        {
            return std::unexpected{InvalidStatement(e.what())};
        }
    }
};

StatementBinder::StatementBinder(const std::function<LogicalPlan(AntlrSQLParser::QueryContext*)>& queryPlanBinder)
    : impl(std::make_unique<Impl>(queryPlanBinder))
{
}

StatementBinder::StatementBinder(StatementBinder&& other) noexcept : impl(std::move(other.impl))
{
}

StatementBinder& StatementBinder::operator=(StatementBinder&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    impl = std::move(other.impl);
    return *this;
}

StatementBinder::~StatementBinder() = default;

std::expected<Statement, Exception> StatementBinder::bind(AntlrSQLParser::StatementContext* statementAST) const
{
    return impl->bind(statementAST);
}

std::expected<std::vector<std::expected<Statement, Exception>>, Exception>
StatementBinder::parseAndBind(const std::string_view statementString) const
{
    try
    {
        antlr4::ANTLRInputStream input(statementString.data(), statementString.length());
        AntlrSQLLexer lexer(&input);
        antlr4::CommonTokenStream tokens(&lexer);
        AntlrSQLParser parser(&tokens);
        /// Remove default error listeners that print to stdout/stderr
        lexer.removeErrorListeners();
        parser.removeErrorListeners();
        /// Enable that antlr throws exceptions on parsing errors
        parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());
        AntlrSQLParser::MultipleStatementsContext* tree = parser.multipleStatements();
        if (tree == nullptr)
        {
            return std::unexpected{InvalidQuerySyntax("{}", statementString)};
        }

        return std::expected<std::vector<std::expected<Statement, Exception>>, Exception>{
            tree->statement() | std::views::transform([this](auto* statementAST) { return impl->bind(statementAST); })
            | std::ranges::to<std::vector>()};
    }
    catch (antlr4::ParseCancellationException& e)
    {
        return std::unexpected{InvalidQuerySyntax("{}", e)};
    }
}

std::expected<Statement, Exception> StatementBinder::parseAndBindSingle(std::string_view statementStrings) const
{
    auto allParsed = parseAndBind(statementStrings);
    if (allParsed.has_value())
    {
        if (allParsed->size() > 1)
        {
            return std::unexpected{InvalidQuerySyntax("Expected a single statement, but got multiple")};
        }
        if (allParsed->empty())
        {
            return std::unexpected{InvalidQuerySyntax("Expected a single statement, but got none")};
        }
        return allParsed->at(0);
    }
    return std::unexpected{allParsed.error()};
}

std::ostream& operator<<(std::ostream& os, const CreatePhysicalSourceStatement& obj)
{
    return os << fmt::format(
               "CreatePhysicalSourceStatement: attachedTo: {} sourceType: {} host: {} sourceConfig: {} parserConfig: {} ifNotExists: {}",
               obj.attachedTo,
               obj.sourceType,
               obj.host ? obj.host->getRawValue() : "<none>",
               obj.sourceConfig,
               obj.parserConfig,
               obj.ifNotExists);
}

/// NOLINTEND(readability-convert-member-functions-to-static)
}
