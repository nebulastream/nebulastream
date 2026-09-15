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

#include <Binder.hpp>

#include <cstdint>
#include <expected>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <CharStream.h>
#include <Token.h>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Strings.hpp>
#include <misc/Interval.h>
#include <ErrorHandling.hpp>
#include <Reply.hpp>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>

namespace NES::ConnTest
{

std::optional<ConnectorKind> parseConnectorKind(const std::string_view text)
{
    return magic_enum::enum_cast<ConnectorKind>(text, magic_enum::case_insensitive);
}

namespace
{

/// The generated grammar type is reached through the parser wrapper's header, not the generated header.
/// NOLINTNEXTLINE(misc-include-cleaner)
using BoundStatementContexts = std::vector<NES::AntlrSQLQueryParser::ManagedAntlrParser::ManagedContext<AntlrSQLParser::StatementContext>>;

/// For example `config does not bind: creating the physical source failed with InvalidConfigParameter (1000): ...`.
std::unexpected<ErrorInfo> bindError(const std::string_view step, const std::string_view detail)
{
    return std::unexpected{harnessErrorInfo("bind", "InvalidConfig", fmt::format("config does not bind: {} {}", step, detail))};
}

std::unexpected<ErrorInfo> bindError(const std::string_view step, const Exception& exception)
{
    auto error = bindError(
        step,
        fmt::format(
            "failed with {} ({}): {}",
            errorCodeName(exception.code()),
            static_cast<int>(exception.code()),
            trimWhiteSpaces(exception.what())));
    error.error().code = static_cast<std::uint16_t>(exception.code());
    return error;
}

std::expected<BoundStatementContexts, Exception> parseProgram(const std::string& sql)
{
    auto parser = NES::AntlrSQLQueryParser::ManagedAntlrParser::create(sql);
    try
    {
        return parser->parseMultiple();
    }
    catch (const Exception& ex)
    {
        return std::unexpected{ex};
    }
}

/// The statement as it is written in the config.
std::string writtenText(const AntlrSQLParser::StatementContext& statement)
{
    const auto* start = statement.getStart();
    return start->getInputStream()->getText(antlr4::misc::Interval{start->getStartIndex(), statement.getStop()->getStopIndex()});
}

}

std::expected<SourceDescriptor, ErrorInfo> bindSourceStatements(const std::string& sql)
{
    const auto statements = parseProgram(sql);
    if (not statements)
    {
        return bindError("parsing the SQL", statements.error());
    }

    const StatementBinder binder{[](auto&& ctx)
                                 { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(ctx)>(ctx)); }};

    std::unordered_map<Identifier, LogicalSource> logicalSources;
    std::optional<SourceDescriptor> physicalDescriptor;
    for (const auto& statementContext : *statements)
    {
        auto bound = binder.bind(statementContext.get());
        if (!bound.has_value())
        {
            return bindError("binding a statement", bound.error());
        }
        if (const auto* logical = std::get_if<CreateLogicalSourceStatement>(&*bound))
        {
            if (!logicalSources.try_emplace(logical->name, logical->name, logical->schema).second)
            {
                return bindError("creating the logical source", SourceAlreadyExists("{}", logical->name));
            }
            continue;
        }
        if (const auto* phys = std::get_if<CreatePhysicalSourceStatement>(&*bound))
        {
            const auto logicalSource = logicalSources.find(phys->attachedTo);
            if (logicalSource == logicalSources.end())
            {
                return bindError("creating the physical source", UnknownSourceName("{}", phys->attachedTo));
            }
            try
            {
                /// Connectors read their endpoint from config and conn-test does no placement, so a missing host can stay invalid.
                auto created = SourceDescriptor::create(
                    PhysicalSourceId{PhysicalSourceId::INITIAL},
                    logicalSource->second,
                    phys->sourceType,
                    phys->host.value_or(Host{Host::INVALID}),
                    phys->sourceConfig,
                    phys->parserConfig,
                    false);
                if (!created.has_value())
                {
                    return bindError("creating the physical source", created.error());
                }
                physicalDescriptor.emplace(std::move(*created));
            }
            catch (const Exception& ex)
            {
                /// Some connectors validate their config here by throwing, not by returning an error.
                return bindError("creating the physical source", ex);
            }
        }
    }
    if (!physicalDescriptor)
    {
        return bindError("creating the physical source", "failed: the config has no CREATE PHYSICAL SOURCE statement");
    }
    return std::move(*physicalDescriptor);
}

std::expected<SinkDescriptor, ErrorInfo> bindSinkStatements(const std::string& sql)
{
    const auto statements = parseProgram(sql);
    if (not statements)
    {
        return bindError("parsing the SQL", statements.error());
    }
    /// The harness runs one sink per session, so it rejects a config with more than one statement.
    if (statements->size() != 1)
    {
        auto detail = fmt::format("failed: expected a single CREATE SINK statement, but the config has {}", statements->size());
        if (not statements->empty())
        {
            detail += fmt::format(
                ": {}",
                fmt::join(*statements | std::views::transform([](const auto& statement) { return writtenText(*statement.get()); }), "; "));
        }
        return bindError("creating the sink", detail);
    }

    const StatementBinder binder{[](auto&& ctx)
                                 { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(ctx)>(ctx)); }};
    auto bound = binder.bind(statements->front().get());
    if (!bound.has_value())
    {
        return bindError("binding a statement", bound.error());
    }
    const auto* sink = std::get_if<CreateSinkStatement>(&*bound);
    if (sink == nullptr)
    {
        return bindError("creating the sink", "failed: the statement is not a CREATE SINK");
    }
    try
    {
        /// The harness runs the sink in its own process and ignores placement, so any host works.
        auto created = SinkDescriptor::createNamed(
            SinkId{SinkId::INITIAL},
            sink->name,
            sink->sinkType,
            sink->schema,
            sink->host.value_or(Host{"localhost"}),
            sink->sinkConfig,
            sink->formatConfig);
        if (toUpperCase(created.getFormatType()) != "JSON")
        {
            return bindError(
                "checking the output format",
                fmt::format("failed: the sink receives JSONL, so its OUTPUT_FORMAT must be 'JSON', not '{}'", created.getFormatType()));
        }
        return created;
    }
    catch (const Exception& ex)
    {
        /// Some connectors validate their config here by throwing, not by returning an error.
        return bindError("creating the sink", ex);
    }
}

}
