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
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <functional>
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
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>

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
#include <Sources/SourceCatalog.hpp>
#include <Util/URI.hpp>
#include <ErrorHandling.hpp>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <CommonParserFunctions.hpp>

namespace NES
{


/// NOLINTBEGIN(readability-convert-member-functions-to-static)
class StatementBinder::Impl
{
    std::shared_ptr<const SourceCatalog> sourceCatalog;
    std::function<LogicalPlan(AntlrSQLParser::QueryContext*)> queryBinder;

public:
    Impl(
        const std::shared_ptr<const SourceCatalog>& sourceCatalog,
        const std::function<LogicalPlan(AntlrSQLParser::QueryContext*)>& queryBinder)
        : sourceCatalog(sourceCatalog), queryBinder(queryBinder)
    {
    }

    ~Impl() = default;

    StatementOutputFormat bindFormat(AntlrSQLParser::ShowFormatContext* formatAST) const
    {
        if (formatAST->TEXT() != nullptr)
        {
            return StatementOutputFormat::TEXT;
        }
        if (formatAST->JSON() != nullptr)
        {
            return StatementOutputFormat::JSON;
        }
        INVARIANT(false, "Invalid format type, is the binder out of sync or was a nullptr passed?");
        std::unreachable();
    }

    CreateLogicalSourceStatement
    bindCreateLogicalSourceStatement(AntlrSQLParser::CreateLogicalSourceDefinitionContext* logicalSourceDefAST) const
    {
        const auto sourceName = bindIdentifier(logicalSourceDefAST->sourceName->strictIdentifier());
        const auto schema = bindSchema(logicalSourceDefAST->schemaDefinition());
        return CreateLogicalSourceStatement{.name = sourceName, .schema = schema};
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
            .attachedTo = logicalSourceName, .sourceType = type, .host = host, .sourceConfig = sourceConfig, .parserConfig = parserConfig};
    }

    CreateWorkerStatement bindCreateWorkerStatement(AntlrSQLParser::CreateWorkerDefinitionContext* workerDefAST) const
    {
        auto configs = (workerDefAST->optionsClause() != nullptr)
            ? bindConfigOptionsWithDuplicates(workerDefAST->optionsClause()->options->namedConfigExpression())
            : ConfigMultiMap{};

        auto capacity = [&] -> std::optional<size_t>
        {
            auto it = std::ranges::find_if(
                configs,
                [](const auto& key) { return key.first.size() == 1 && *std::ranges::begin(key.first) == Identifier::parse("CAPACITY"); });
            if (it != configs.end())
            {
                auto* literalOpt = std::get_if<Literal>(&it->second);
                if (literalOpt && std::holds_alternative<uint64_t>(*literalOpt))
                {
                    return static_cast<size_t>(std::get<uint64_t>(*literalOpt));
                }
                throw InvalidQuerySyntax("Capacity must be an unsigned integer literal");
            }
            return std::nullopt;
        }();

        auto dataAddress = [&] -> std::string
        {
            auto it = std::ranges::find_if(
                configs,
                [](const auto& key) { return key.first.size() == 1 && *std::ranges::begin(key.first) == Identifier::parse("DATA"); });
            if (it != configs.end())
            {
                const Literal* literalOpt = std::get_if<Literal>(&it->second);
                if (literalOpt && std::holds_alternative<std::string>(*literalOpt))
                {
                    return URI(std::get<std::string>(*literalOpt)).toString();
                }
                throw InvalidQuerySyntax("DATA must be a string literal");
            }
            return {};
        }();

        auto downStreams = [&] -> std::vector<std::string>
        {
            return configs
                | std::views::filter(
                       [](const auto& option)
                       { return option.first.size() == 1 && *std::ranges::begin(option.first) == Identifier::parse("DOWNSTREAM"); })
                | std::views::values
                | std::views::transform(
                       [](const auto& value)
                       {
                           const Literal* literalOpt = std::get_if<Literal>(&value);
                           if (literalOpt && std::holds_alternative<std::string>(*literalOpt))
                           {
                               return URI(std::get<std::string>(*literalOpt)).toString();
                           }
                           throw InvalidQuerySyntax("DOWNSTREAM must be a string literal");
                       })
                | std::ranges::to<std::vector<std::string>>();
        }();


        return CreateWorkerStatement{
            .host = URI(bindStringLiteral(workerDefAST->hostaddr)).toString(),
            .dataAddress = std::move(dataAddress),
            .capacity = capacity,
            .downstream = downStreams,
            .config = {}};
    }

    CreateSinkStatement bindCreateSinkStatement(AntlrSQLParser::CreateSinkDefinitionContext* sinkDefAST) const
    {
        const auto sinkName = bindIdentifier(sinkDefAST->sinkName->strictIdentifier());
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
        if (const auto formatConfigIter = configOptions.find(Identifier::parse("OUTPUT_FORMATTER"));
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
            .formatConfig = formatOptions};
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
            .outputs = std::move(outputSchemaExp).value()};
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
        throw InvalidStatement("Unrecognized CREATE statement");
    }

    ShowLogicalSourcesStatement bindShowLogicalSourcesStatement(
        const AntlrSQLParser::ShowFilterContext* showFilter, AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        const std::optional<StatementOutputFormat> format
            = showFormat != nullptr ? std::make_optional(bindFormat(showFormat)) : std::nullopt;
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
            return ShowLogicalSourcesStatement{.name = bindIdentifier(std::get<std::string>(value)), .format = format};
        }
        return ShowLogicalSourcesStatement{.name = std::nullopt, .format = format};
    }

    ShowPhysicalSourcesStatement bindShowPhysicalSourcesStatement(
        const AntlrSQLParser::ShowFilterContext* showFilter,
        const AntlrSQLParser::ShowPhysicalSourcesSubjectContext* physicalSourcesSubject,
        AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        std::optional<LogicalSourceName> logicalSourceName{};
        const std::optional<StatementOutputFormat> format
            = showFormat != nullptr ? std::make_optional(bindFormat(showFormat)) : std::nullopt;
        if (physicalSourcesSubject->logicalSourceName != nullptr)
        {
            logicalSourceName = LogicalSourceName(bindIdentifier(physicalSourcesSubject->logicalSourceName));
        }
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            static const auto IdIdentifier = Identifier::parse("ID");
            if (attr != IdIdentifier)
            {
                throw InvalidQuerySyntax("Filter for SHOW PHYSICAL SOURCES must be on id attribute");
            }
            if (not std::holds_alternative<uint64_t>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW PHYSICAL SOURCES must be an unsigned integer");
            }
            return ShowPhysicalSourcesStatement{.logicalSource = logicalSourceName, .id = std::get<uint64_t>(value), .format = format};
        }
        return ShowPhysicalSourcesStatement{.logicalSource = logicalSourceName, .id = std::nullopt, .format = format};
    }

    ShowSinksStatement
    bindShowSinksStatement(const AntlrSQLParser::ShowFilterContext* showFilter, AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        const std::optional<StatementOutputFormat> format
            = showFormat != nullptr ? std::make_optional(bindFormat(showFormat)) : std::nullopt;
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
            return ShowSinksStatement{.name = bindIdentifier(std::get<std::string>(value)), .format = format};
        }
        return ShowSinksStatement{.name = std::nullopt, .format = format};
    }

    ShowQueriesStatement
    bindShowQueriesStatement(const AntlrSQLParser::ShowFilterContext* showFilter, AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        const std::optional<StatementOutputFormat> format
            = showFormat != nullptr ? std::make_optional(bindFormat(showFormat)) : std::nullopt;
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            static const auto IdIdentifier = Identifier::parse("ID");
            if (attr != IdIdentifier)
            {
                throw InvalidQuerySyntax("Filter for SHOW QUERIES must be on id attribute");
            }
            if (not std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW QUERIES must be a string");
            }
            return ShowQueriesStatement{.id = DistributedQueryId{std::get<std::string>(value)}, .format = format};
        }
        return ShowQueriesStatement{.id = std::nullopt, .format = format};
    }

    Statement bindShowStatement(AntlrSQLParser::ShowStatementContext* showAST) const
    {
        auto* showFilter = showAST->showFilter();

        if (const auto* logicalSourcesSubject = dynamic_cast<AntlrSQLParser::ShowLogicalSourcesSubjectContext*>(showAST->showSubject());
            logicalSourcesSubject != nullptr)
        {
            return bindShowLogicalSourcesStatement(showFilter, showAST->showFormat());
        }
        if (auto* physicalSourcesSubject = dynamic_cast<AntlrSQLParser::ShowPhysicalSourcesSubjectContext*>(showAST->showSubject());
            physicalSourcesSubject != nullptr)
        {
            return bindShowPhysicalSourcesStatement(showFilter, physicalSourcesSubject, showAST->showFormat());
        }
        if (const auto* queriesSubject = dynamic_cast<AntlrSQLParser::ShowQueriesSubjectContext*>(showAST->showSubject());
            queriesSubject != nullptr)
        {
            return bindShowQueriesStatement(showFilter, showAST->showFormat());
        }
        if (const auto* sinksSubject = dynamic_cast<AntlrSQLParser::ShowSinksSubjectContext*>(showAST->showSubject());
            sinksSubject != nullptr)
        {
            return bindShowSinksStatement(showFilter, showAST->showFormat());
        }
        if (const auto* modelsSubject = dynamic_cast<AntlrSQLParser::ShowModelsSubjectContext*>(showAST->showSubject());
            modelsSubject != nullptr)
        {
            const std::optional<StatementOutputFormat> format
                = showAST->showFormat() != nullptr ? std::make_optional(bindFormat(showAST->showFormat())) : std::nullopt;
            return ShowModelsStatement{.format = format};
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

    static DropLogicalSourceStatement bindDropLogicalSource(const std::pair<Identifier, Literal>& filter)
    {
        return DropLogicalSourceStatement{
            LogicalSourceName(Identifier::parse(requireFilterValue<std::string>(filter, "NAME", "a string", "DROP LOGICAL SOURCE")))};
    }

    [[nodiscard]] DropPhysicalSourceStatement bindDropPhysicalSource(const std::pair<Identifier, Literal>& filter) const
    {
        const auto id = requireFilterValue<uint64_t>(filter, "ID", "an unsigned integer", "DROP PHYSICAL SOURCE");
        if (const auto physicalSource = sourceCatalog->getPhysicalSource(PhysicalSourceId{id}))
        {
            return DropPhysicalSourceStatement{*physicalSource};
        }
        throw UnknownSourceName("There is no physical source with id {}", id);
    }

    static DropQueryStatement bindDropQuery(const std::pair<Identifier, Literal>& filter)
    {
        return DropQueryStatement{.id = DistributedQueryId(requireFilterValue<std::string>(filter, "ID", "a string", "DROP QUERY"))};
    }

    static DropSinkStatement bindDropSink(const std::pair<Identifier, Literal>& filter)
    {
        return DropSinkStatement{Identifier::parse(requireFilterValue<std::string>(filter, "NAME", "a string", "DROP SINK"))};
    }

    static DropModelStatement bindDropModel(const std::pair<Identifier, Literal>& filter)
    {
        return DropModelStatement{.name = requireFilterValue<std::string>(filter, "NAME", "a string", "DROP MODEL")};
    }

    Statement bindDropStatement(AntlrSQLParser::DropStatementContext* dropAst) const
    {
        const auto* const dropFilter = dropAst->dropFilter();
        PRECONDITION(dropFilter != nullptr, "Drop statement must have a WHERE filter");
        const auto filter = bindDropFilter(dropFilter);
        auto* const subject = dropAst->dropSubject();

        if (auto* const dropSourceAst = subject->dropSource())
        {
            if (dropSourceAst->dropLogicalSourceSubject() != nullptr)
            {
                return bindDropLogicalSource(filter);
            }
            if (dropSourceAst->dropPhysicalSourceSubject() != nullptr)
            {
                return bindDropPhysicalSource(filter);
            }
        }
        if (subject->dropQuery() != nullptr)
        {
            return bindDropQuery(filter);
        }
        if (subject->dropSink() != nullptr)
        {
            return bindDropSink(filter);
        }
        if (subject->dropModel() != nullptr)
        {
            return bindDropModel(filter);
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
            if (auto* const explainStatementAST = statementAST->explainStatement())
            {
                return bindExplainStatement(explainStatementAST);
            }
            if (auto* const queryAst = statementAST->queryWithOptions(); queryAst != nullptr)
            {
                std::optional<DistributedQueryId> queryId;
                if (queryAst->optionsClause() != nullptr)
                {
                    auto options = bindConfigOptions(queryAst->optionsClause()->options->namedConfigExpression());
                    static const auto QueryIdentifier = Identifier::parse("QUERY");
                    static const auto IdIdentifier = Identifier::parse("ID");
                    if (auto optionsIter = options.find(QueryIdentifier); optionsIter != options.end())
                    {
                        if (auto idIter = optionsIter->second.find(IdIdentifier); idIter != optionsIter->second.end())
                        {
                            auto* literal = std::get_if<Literal>(&idIter->second);
                            if ((literal == nullptr) || !std::holds_alternative<std::string>(*literal))
                            {
                                throw InvalidQuerySyntax("Query id must be a string");
                            }
                            queryId = DistributedQueryId(std::get<std::string>(*literal));
                        }
                    }
                }
                return QueryStatement{.plan = queryBinder(queryAst->query()), .id = queryId};
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

StatementBinder::StatementBinder(
    const std::shared_ptr<const SourceCatalog>& sourceCatalog,
    const std::function<LogicalPlan(AntlrSQLParser::QueryContext*)>& queryPlanBinder)
    : impl(std::make_unique<Impl>(sourceCatalog, queryPlanBinder))
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
               "CreatePhysicalSourceStatement: attachedTo: {} sourceType: {} host: {} sourceConfig: {} parserConfig: {}",
               obj.attachedTo,
               obj.sourceType,
               obj.host ? obj.host->getRawValue() : "<none>",
               obj.sourceConfig,
               obj.parserConfig);
}

/// NOLINTEND(readability-convert-member-functions-to-static)
}
