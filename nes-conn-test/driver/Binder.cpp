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

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Schema/Schema.hpp>
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

}

std::expected<SourceDescriptor, ErrorInfo> bindSourceStatements(const std::string& sql)
{
    auto sourceCatalogHandle = SourceCatalog::create();
    auto sourceCatalog = copyPtr(sourceCatalogHandle);

    /// Connectors read their endpoint from config and conn-test does no placement, so a missing host defaults to INVALID.
    const StatementBinder binder{
        sourceCatalog,
        /*sinkCatalog=*/nullptr,
        /*modelCatalog=*/nullptr,
        Schema<ConfigFieldDefault, Ordered>{
            ConfigFieldDefault{QualifiedIdentifier::parse("SOURCE.HOST"), ConfigLiteral{std::string{Host::INVALID}}}}};

    std::optional<SourceDescriptor> physicalDescriptor;
    std::optional<ErrorInfo> stepFailure;

    const auto parseRes = binder.parseAndBindEach(
        sql,
        [&](std::expected<Statement, Exception> bound)
        {
            if (stepFailure.has_value())
            {
                return;
            }
            if (!bound.has_value())
            {
                stepFailure = bindError("binding a statement", bound.error()).error();
                return;
            }
            if (auto* logical = std::get_if<CreateLogicalSourceStatement>(&*bound))
            {
                if (!sourceCatalog->addLogicalSource(logical->name, logical->schema))
                {
                    stepFailure = bindError("creating the logical source", fmt::format("failed: {} is declared twice", logical->name)).error();
                }
                return;
            }
            if (auto* phys = std::get_if<CreatePhysicalSourceStatement>(&*bound))
            {
                const auto logicalSource = sourceCatalog->getLogicalSource(phys->logicalSourceName);
                if (!logicalSource)
                {
                    stepFailure
                        = bindError("creating the physical source", fmt::format("failed: no logical source {}", phys->logicalSourceName)).error();
                    return;
                }
                try
                {
                    auto created = sourceCatalog->registerWithLogicalSource(
                        PhysicalSourceBuilder{phys->generalSourceConfig, phys->pluginSourceConfig, phys->pluginInputFormatterConfig},
                        phys->logicalSourceName);
                    if (!created.has_value())
                    {
                        stepFailure = bindError("creating the physical source", created.error()).error();
                        return;
                    }
                    physicalDescriptor.emplace(std::move(*created));
                }
                catch (const Exception& ex)
                {
                    stepFailure = bindError("creating the physical source", ex).error();
                }
            }
        });

    if (!parseRes.has_value())
    {
        return bindError("parsing the SQL", parseRes.error());
    }
    if (stepFailure.has_value())
    {
        return std::unexpected{std::move(*stepFailure)};
    }
    if (!physicalDescriptor)
    {
        return bindError("creating the physical source", "failed: the config has no CREATE PHYSICAL SOURCE statement");
    }
    return std::move(*physicalDescriptor);
}

}
