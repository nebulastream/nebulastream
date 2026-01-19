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
#include <variant>
#include <vector>

#include <Plans/LogicalPlan.hpp>

#include <AntlrSQLParser.h>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/base.h>
#include <ErrorHandling.hpp>

namespace NES
{

using LogicalSourceName = NESStrongStringType<struct LogicalSourceName_, "invalid">;

enum class StatementOutputFormat : uint8_t
{
    JSON,
    TEXT
};

/// The source management statements are directly executed by the binder as we currently do not need to handle them differently between the frontends.
/// Should we require this in the future, we can change these structs to some intermediate representation with which the frontends have to go to the source catalog with.
struct CreateLogicalSourceStatement
{
    std::string name;
    Schema schema;
};

struct CreatePhysicalSourceStatement
{
    LogicalSourceName attachedTo;
    std::string sourceType;
    std::unordered_map<std::string, std::string> sourceConfig;
    std::unordered_map<std::string, std::string> parserConfig;
    friend std::ostream& operator<<(std::ostream& os, const CreatePhysicalSourceStatement& obj);
};

struct CreateSinkStatement
{
    std::string name;
    std::string sinkType;
    Schema schema;
    std::unordered_map<std::string, std::string> sinkConfig;
};

/// ShowLogicalSourcesStatement only contains a name not bound to a logical statement,
/// because searching for a name for which no logical source exists is not a syntax error but just returns an empty result
struct ShowLogicalSourcesStatement
{
    std::optional<std::string> name;
    std::optional<StatementOutputFormat> format;
};

/// ShowPhysicalSourcesStatement, on the other hand, cannot reference a logical source by name that doesn't exist because it is directly
/// referencing a dms object
struct ShowPhysicalSourcesStatement
{
    std::optional<LogicalSourceName> logicalSource;
    std::optional<uint32_t> id;
    std::optional<StatementOutputFormat> format;
};

struct ShowSinksStatement
{
    std::optional<std::string> name;
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
    std::string name;
};

struct QueryStatement
{
    LogicalPlan plan;
    std::optional<size_t> id;
};

struct ExplainQueryStatement
{
    LogicalPlan plan;
};

struct ShowQueriesStatement
{
    std::optional<QueryId> id;
    std::optional<StatementOutputFormat> format;
};

struct DropQueryStatement
{
    QueryId id;
};

struct WorkerStatusStatement
{
};

using Statement = std::variant<
    WorkerStatusStatement,
    CreateLogicalSourceStatement,
    CreatePhysicalSourceStatement,
    CreateSinkStatement,
    ShowLogicalSourcesStatement,
    ShowPhysicalSourcesStatement,
    DropLogicalSourceStatement,
    DropPhysicalSourceStatement,
    DropSinkStatement,
    QueryStatement,
    ExplainQueryStatement,
    ShowQueriesStatement,
    ShowSinksStatement,
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

class StatementBinder
{
    /// PIMPL pattern to hide all the internally used binder member functions
    class Impl;
    std::unique_ptr<Impl> impl;

public:
    explicit StatementBinder(
        const std::shared_ptr<const SourceCatalog>& sourceCatalog,
        const std::function<LogicalPlan(AntlrSQLParser::QueryContext*)>& queryPlanBinder) noexcept;

    StatementBinder(const StatementBinder& other) = delete;
    StatementBinder& operator=(const StatementBinder& other) = delete;
    StatementBinder(StatementBinder&& other) noexcept;
    StatementBinder& operator=(StatementBinder&& other) noexcept;

    /// If the destructor was implicitly defaulted, it would call the destructor of the unique ptr, which would require the definition of Impl.
    /// Deferring the destructor default to the implementation, where also the definition of Impl is, solves this problem.
    ~StatementBinder();
    [[nodiscard]] std::expected<Statement, Exception> bind(AntlrSQLParser::StatementContext* statementAST) const noexcept;
    [[nodiscard]] std::expected<std::vector<std::expected<Statement, Exception>>, Exception>
    parseAndBind(std::string_view statementString) const noexcept;
    [[nodiscard]] std::expected<Statement, Exception> parseAndBindSingle(std::string_view statementStrings) const noexcept;
};
}

namespace fmt
{
template <>
struct formatter<std::unordered_map<std::string, std::string>>
{
    [[nodiscard]] static constexpr auto parse(const format_parse_context& ctx) noexcept -> decltype(ctx.begin()) { return ctx.begin(); }

    static constexpr auto format(const std::unordered_map<std::string, std::string>& mapToFormat, format_context& ctx) noexcept
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
