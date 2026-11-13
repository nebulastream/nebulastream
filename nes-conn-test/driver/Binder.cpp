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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <Reply.hpp>

#include <fmt/format.h>

namespace NES::ConnTest
{
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

}

std::expected<SourceDescriptor, ErrorInfo> bindSourceStatements(const std::string& sql)
{
    const auto statements = parseProgram(sql);
    if (not statements)
    {
        return bindError("parsing the SQL", statements.error());
    }

    auto sourceCatalog = std::make_shared<SourceCatalog>();
    const StatementBinder binder(
        sourceCatalog, [](auto&& ctx) { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(ctx)>(ctx)); });
    /// Connectors read their endpoint from config and conn-test does no placement, so a statement without a host gets any valid one,
    /// as in the REPL with a single worker.
    SourceStatementHandler handler{sourceCatalog, DefaultHost{"localhost"}};

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
            if (auto created = handler(*logical); !created.has_value())
            {
                return bindError("creating the logical source", created.error());
            }
            continue;
        }
        if (const auto* phys = std::get_if<CreatePhysicalSourceStatement>(&*bound))
        {
            try
            {
                auto created = handler(*phys);
                if (!created.has_value())
                {
                    return bindError("creating the physical source", created.error());
                }
                physicalDescriptor.emplace(std::move(created->created));
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
    /// A second statement would be ignored or replace the first, so the config must hold exactly one.
    if (statements->size() != 1)
    {
        return bindError("creating the sink", fmt::format("failed: the config has {} statements, not one CREATE SINK", statements->size()));
    }

    /// The binder needs a source catalog, even though a sink config declares no source.
    const StatementBinder binder(
        std::make_shared<SourceCatalog>(),
        [](auto&& ctx) { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(ctx)>(ctx)); });
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
        /// Any valid host, as for a source.
        SinkStatementHandler handler{std::make_shared<SinkCatalog>(), DefaultHost{"localhost"}};
        auto created = handler(*sink);
        if (!created.has_value())
        {
            return bindError("creating the sink", created.error());
        }
        return std::move(created->created);
    }
    catch (const Exception& ex)
    {
        /// Some connectors validate their config here by throwing, not by returning an error.
        return bindError("creating the sink", ex);
    }
}

}
