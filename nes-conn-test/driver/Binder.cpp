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

#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <Report.hpp>
#include <SinkValidationRegistry.hpp>

#include <fmt/format.h>

namespace NES::ConnTest
{
namespace
{

/// AntlrSQLParser is the generated grammar type, reached intentionally through the AntlrSQLQueryParser.hpp wrapper
/// rather than by including the generated header directly.
/// NOLINTNEXTLINE(misc-include-cleaner)
using BoundStatementContexts = std::vector<NES::AntlrSQLQueryParser::ManagedAntlrParser::ManagedContext<AntlrSQLParser::StatementContext>>;

/// Front half shared by both binders: parse `sql` with the SQL grammar and
/// capture a parse failure into the Report the same way. On a parse error,
/// populates `report.error` (origin = connector, phase = "parse") and returns
/// nullopt. Only the per-statement handling (which catalog the bound statement
/// lands on) differs, so that stays in the two bind* functions below.
std::optional<BoundStatementContexts> parseProgram(const std::string& sql, Report& report)
{
    auto parser = NES::AntlrSQLQueryParser::ManagedAntlrParser::create(sql);
    auto parseResult = [&]
    {
        try
        {
            return parser->parseMultiple();
        }
        catch (const Exception& ex)
        {
            return decltype(parser->parseMultiple()){std::unexpected{ex}};
        }
    }();
    if (not parseResult.has_value())
    {
        captureConnector(report, "parse", parseResult.error());
        return std::nullopt;
    }
    return std::move(parseResult).value();
}

}

std::optional<SourceDescriptor> bindSourceStatements(const std::string& sql, Report& report)
{
    const auto statements = parseProgram(sql, report);
    if (not statements)
    {
        return std::nullopt;
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
            captureConnector(report, "parse", bound.error());
            return std::nullopt;
        }
        if (auto* logical = std::get_if<CreateLogicalSourceStatement>(&*bound))
        {
            if (!sourceCatalog->addLogicalSource(logical->name, logical->schema))
            {
                captureHarness(report, "descriptor", "HarnessProtocol", fmt::format("duplicate logical source: {}", logical->name));
                return std::nullopt;
            }
            continue;
        }
        if (auto* phys = std::get_if<CreatePhysicalSourceStatement>(&*bound))
        {
            const auto logicalSource = sourceCatalog->getLogicalSource(phys->attachedTo.getRawValue());
            if (!logicalSource)
            {
                captureHarness(
                    report, "descriptor", "HarnessProtocol", fmt::format("unknown logical source: {}", phys->attachedTo.getRawValue()));
                return std::nullopt;
            }
            try
            {
                /// The descriptor's host is a worker-placement hint the connectors ignore (they read
                /// their endpoint from config), and conn-test runs no placement — so it is inert here.
                /// Honor an explicitly-set host, but default to the Host::INVALID sentinel when the
                /// config omits it (symmetric with the sink path below).
                auto created = sourceCatalog->addPhysicalSource(
                    *logicalSource, phys->sourceType, phys->host.value_or(Host{Host::INVALID}), phys->sourceConfig, phys->parserConfig);
                if (!created.has_value())
                {
                    captureConnector(report, "descriptor", created.error());
                    return std::nullopt;
                }
                physicalDescriptor.emplace(std::move(*created));
            }
            catch (const Exception& ex)
            {
                /// Some connectors validate per-source
                /// config in addPhysicalSource and surface failures via throw
                /// rather than std::unexpected.
                captureConnector(report, "descriptor", ex);
                return std::nullopt;
            }
        }
    }
    if (!physicalDescriptor)
    {
        captureHarness(report, "descriptor", "HarnessProtocol", "config file produced no CREATE PHYSICAL SOURCE statement");
    }
    return physicalDescriptor;
}

std::optional<SinkDescriptor> bindSinkStatements(const std::string& sql, Report& report)
{
    const auto statements = parseProgram(sql, report);
    if (not statements)
    {
        return std::nullopt;
    }

    /// A sink config is exactly one CREATE SINK statement — no logical/physical
    /// pre-statements like the source side. Require that exactly, so a stray or
    /// duplicated statement is a clear config error rather than silently binding
    /// whichever sink happens to come last.
    if (statements->size() != 1)
    {
        captureHarness(
            report,
            "descriptor",
            "HarnessProtocol",
            fmt::format("sink config must be exactly one CREATE SINK statement, found {}", statements->size()));
        return std::nullopt;
    }

    /// StatementBinder requires a SourceCatalog even when only sinks are bound; the sink lives on a
    /// local SinkCatalog. The query-plan binder is the same callback bindSourceStatements installs —
    /// sink configs never produce a QueryStatement, but the binder constructor demands it.
    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto sinkCatalog = std::make_shared<SinkCatalog>();
    const StatementBinder binder(
        sourceCatalog, [](auto&& ctx) { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(ctx)>(ctx)); });

    auto bound = binder.bind(statements->front().get());
    if (not bound.has_value())
    {
        captureConnector(report, "parse", bound.error());
        return std::nullopt;
    }
    auto* sink = std::get_if<CreateSinkStatement>(&*bound);
    if (sink == nullptr)
    {
        captureHarness(report, "descriptor", "HarnessProtocol", "sink config statement is not a CREATE SINK statement");
        return std::nullopt;
    }

    /// Report an unregistered sink type as UnknownSinkType so the conn-test registration
    /// pre-flight can tell "plugin not activated" apart from a genuine bad config. We make the
    /// distinction here, against the SinkValidationRegistry, rather than relying on
    /// SinkCatalog::addSinkDescriptor — which surfaces an unknown type as a generic
    /// InvalidConfigParameter — so the probe's contract stays a conn-test concern and needs no
    /// change to shared sink semantics.
    if (not SinkValidationRegistry::instance().contains(sink->sinkType))
    {
        captureConnector(
            report,
            "descriptor",
            UnknownSinkType("The sink type '{}' is not registered. If it is a plugin, make sure you activated it.", sink->sinkType));
        return std::nullopt;
    }
    try
    {
        /// Default the inert placement host as on the source path above — the sink connectors
        /// ignore it (they read their endpoint from config) and conn-test runs no placement.
        auto created = sinkCatalog->addSinkDescriptor(
            sink->name, sink->schema, sink->sinkType, sink->host.value_or(Host{Host::INVALID}), sink->sinkConfig, sink->formatConfig);
        if (not created.has_value())
        {
            captureConnector(report, "descriptor", created.error());
            return std::nullopt;
        }
        return std::move(*created);
    }
    catch (const Exception& ex)
    {
        captureConnector(report, "descriptor", ex);
        return std::nullopt;
    }
}

}
