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

#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include <AntlrSQLParser.h>
#include <Configurations/ConfigField.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/base.h>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>
#include <ModelCatalog.hpp>

namespace NES
{

using DistributedQueryId = NESStrongStringType<struct DistributedQueryId_, "invalid">; /// NOLINT(misc-include-cleaner)
using LogicalSourceName = Identifier;

enum class StatementOutputFormat : uint8_t
{
    JSON,
    TEXT
};

struct CreateLogicalSourceStatement
{
    Identifier name;
    Schema<UnqualifiedUnboundField, Ordered> schema;
};

struct CreatePhysicalSourceStatement
{
    Identifier logicalSourceName;
    GeneralSourceConfig generalSourceConfig;
    PluginSourceConfiguration pluginSourceConfig;
    InputFormatterDescriptor pluginInputFormatterConfig;
    friend std::ostream& operator<<(std::ostream& os, const CreatePhysicalSourceStatement& obj);
};

struct CreateSinkStatement
{
    Identifier name;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    GeneralSinkConfig generalSinkConfig;
    PluginSinkConfiguration pluginSinkConfig;
    OutputFormatterDescriptor outputFormatterDescriptor;
};

struct ShowLogicalSourcesStatement
{
    std::optional<Identifier> name;
    std::optional<StatementOutputFormat> format;
};

struct ShowPhysicalSourcesStatement
{
    std::optional<LogicalSourceName> logicalSource;
    std::optional<uint32_t> id;
    std::optional<StatementOutputFormat> format;
};

struct ShowSinksStatement
{
    std::optional<Identifier> name;
    std::optional<StatementOutputFormat> format;
};

struct DropLogicalSourceStatement
{
    LogicalSourceName source;
};

struct DropPhysicalSourceStatement
{
    SourceDescriptor descriptor;
};

struct DropSinkStatement
{
    Identifier name;
};

struct QueryStatement
{
    LogicalPlan plan;
    std::optional<DistributedQueryId> id;
};

enum class ExplainFormat : uint8_t
{
    Visual,
    Text,
    Verbose
};

enum class ExplainStage : uint8_t
{
    Logical,
    Optimized,
    Distributed
};

struct ExplainQueryStatement
{
    LogicalPlan plan;
    ExplainFormat explainFormat = ExplainFormat::Visual;
    std::unordered_set<ExplainStage> explainStages = {ExplainStage::Logical, ExplainStage::Optimized, ExplainStage::Distributed};
};

struct ShowQueriesStatement
{
    std::optional<DistributedQueryId> id;
    std::optional<StatementOutputFormat> format;
};

struct DropQueryStatement
{
    DistributedQueryId id;
};

struct CreateModelStatement
{
    std::string name;
    std::string path;
    Schema<UnqualifiedUnboundField, Ordered> inputs;
    Schema<UnqualifiedUnboundField, Ordered> outputs;
};

struct ShowModelsStatement
{
    std::optional<StatementOutputFormat> format;
};

struct ShowVersionStatement
{
    std::optional<StatementOutputFormat> format;
};

struct DropModelStatement
{
    std::string name;
};

struct WorkerStatusStatement
{
    std::vector<std::string> host;
};

struct CreateWorkerStatement
{
    std::string host;
    std::string dataAddress;
    std::optional<size_t> maxOperators;
    std::vector<std::string> downstream;
    std::unordered_map<std::string, std::string> config; /// Flat dot-separated config map (e.g., "worker.receiver_queue_size" -> "2")
};

struct DropWorkerStatement
{
    std::string host;
};

using Statement = std::variant<
    WorkerStatusStatement,
    CreateWorkerStatement,
    DropWorkerStatement,
    CreateLogicalSourceStatement,
    CreatePhysicalSourceStatement,
    CreateSinkStatement,
    CreateModelStatement,
    ShowLogicalSourcesStatement,
    ShowPhysicalSourcesStatement,
    DropLogicalSourceStatement,
    DropPhysicalSourceStatement,
    DropSinkStatement,
    DropModelStatement,
    QueryStatement,
    ExplainQueryStatement,
    ShowQueriesStatement,
    ShowSinksStatement,
    ShowModelsStatement,
    ShowVersionStatement,
    DropQueryStatement>;

inline std::optional<StatementOutputFormat> getOutputFormat(const Statement& statement)
{
    /// NOLINTNEXTLINE(fuchsia-trailing-return)
    auto visitor = [](const auto& visitedStatement) -> std::optional<StatementOutputFormat>
    {
        if constexpr (requires { visitedStatement.format; })
        {
            return visitedStatement.format;
        }
        else
        {
            return std::nullopt;
        }
    };
    return std::visit(visitor, statement);
}

struct DefaultHost
{
    std::string hostName;
};

struct RequireHostConfig
{
};

using HostPolicy = std::variant<RequireHostConfig, DefaultHost>;

class StatementBinder
{
    /// PIMPL pattern to hide all internally used binder member functions and scope structures.
    class Impl;
    std::unique_ptr<Impl> impl;

public:
    explicit StatementBinder(
        std::shared_ptr<const SourceCatalog> sourceCatalog = nullptr,
        std::shared_ptr<const SinkCatalog> sinkCatalog = nullptr,
        std::shared_ptr<const ModelCatalog> modelCatalog = nullptr,
        Schema<ConfigFieldDefault, Ordered> defaultConfigValues = {},
        Schema<ConfigFieldTransformation, Unordered> configTransformations = {});

    StatementBinder(const StatementBinder& other) = delete;
    StatementBinder& operator=(const StatementBinder& other) = delete;
    StatementBinder(StatementBinder&& other) noexcept;
    StatementBinder& operator=(StatementBinder&& other) noexcept;
    ~StatementBinder();

    [[nodiscard]] std::expected<Statement, Exception> bind(AntlrSQLParser::StatementContext* statementAST) const;
    [[nodiscard]] std::expected<std::vector<std::expected<Statement, Exception>>, Exception>
    parseAndBind(std::string_view statementString) const;
    [[nodiscard]] std::expected<Statement, Exception> parseAndBindSingle(std::string_view statementStrings) const;

    /// Streaming multi-statement parse & bind: invokes `onStatement` sequentially so DDL statements applied by the
    /// caller update the catalogs before subsequent queries in the same script are bound.
    [[nodiscard]] std::expected<void, Exception> parseAndBindEach(
        std::string_view statementString,
        const std::function<void(std::expected<Statement, Exception>)>& onStatement) const;

    [[nodiscard]] std::expected<LogicalPlan, Exception> parseAndBindQuery(std::string_view queryString) const;
    [[nodiscard]] static LogicalPlan createLogicalQueryPlanFromSQLString(std::string_view queryString);
};

}

namespace fmt
{
template <>
struct formatter<std::unordered_map<std::string, std::string>>
{
    [[nodiscard]] static constexpr auto parse(const format_parse_context& ctx) noexcept -> decltype(ctx.begin()) { return ctx.begin(); }

    static constexpr auto format(const std::unordered_map<std::string, std::string>& mapToFormat, format_context& ctx)
        -> decltype(ctx.out())
    {
        auto out = ctx.out();
        out = format_to(out, "{{");
        bool first = true;
        for (const auto& [fst, snd] : mapToFormat)
        {
            if (!first)
            {
                out = format_to(out, ", ");
            }
            /// Note: fmt::format_to correctly handles escaping for strings by default.
            out = fmt::format_to(out, R"("{}": "{}")", fst, snd);
            first = false;
        }
        out = format_to(out, "}}");
        return out;
    }
};
}

FMT_OSTREAM(NES::CreateLogicalSourceStatement);
FMT_OSTREAM(NES::CreatePhysicalSourceStatement);
FMT_OSTREAM(NES::DropLogicalSourceStatement);
FMT_OSTREAM(NES::DropPhysicalSourceStatement);
FMT_OSTREAM(NES::DropQueryStatement);
FMT_OSTREAM(NES::WorkerStatusStatement);
FMT_OSTREAM(NES::ExplainQueryStatement);
FMT_OSTREAM(NES::CreateWorkerStatement);
FMT_OSTREAM(NES::DropWorkerStatement);
