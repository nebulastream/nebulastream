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

#include <Identifiers/Identifiers.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
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

    std::optional<SourceDescriptor> physicalDescriptor;
    for (const auto& statementContext : *statements)
    {
        auto bound = binder.bind(statementContext.get());
        if (!bound.has_value())
        {
            return bindError("binding a statement", bound.error());
        }
        if (auto* logical = std::get_if<CreateLogicalSourceStatement>(&*bound))
        {
            if (!sourceCatalog->addLogicalSource(logical->name, logical->schema))
            {
                return bindError("creating the logical source", fmt::format("failed: {} is declared twice", logical->name));
            }
            continue;
        }
        if (auto* phys = std::get_if<CreatePhysicalSourceStatement>(&*bound))
        {
            const auto logicalSource = sourceCatalog->getLogicalSource(phys->attachedTo);
            if (!logicalSource)
            {
                return bindError("creating the physical source", fmt::format("failed: no logical source {}", phys->attachedTo));
            }
            try
            {
                /// Connectors read their endpoint from config and conn-test does no placement, so a missing host can stay invalid.
                auto created = sourceCatalog->addPhysicalSource(
                    *logicalSource, phys->sourceType, phys->host.value_or(Host{Host::INVALID}), phys->sourceConfig, phys->parserConfig);
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

}
