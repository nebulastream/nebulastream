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
#include <AntlrSQLParser.h>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sources/LogicalSource.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/base.h>
#include <ErrorHandling.hpp>

namespace NES {
    using LogicalSourceName = Identifier;

    /// The source management statements are directly executed by the binder as we currently do not need to handle them differently between the frontends.
/// Should we require this in the future, we can change these structs to some intermediate representation with which the frontends have to go to the source catalog with.
struct CreateLogicalSourceStatement
{
    Identifier name;
    Schema<UnqualifiedUnboundField, Ordered> schema;
};

/// ShowLogicalSourcesStatement only contains a name not bound to a logical statement,
/// because searching for a name for which no logical source exists is not a syntax error but just returns an empty result
struct ShowLogicalSourcesStatement {
    std::optional<Identifier> name;
};

struct DropLogicalSourceStatement {
    LogicalSourceName source;
};

struct CreatePhysicalSourceStatement
{
    LogicalSourceName attachedTo;
    Identifier sourceType;
    std::optional<Host> host;
    std::unordered_map<Identifier, std::string> sourceConfig;
    std::unordered_map<Identifier, std::string> parserConfig;
    friend std::ostream& operator<<(std::ostream& os, const CreatePhysicalSourceStatement& obj);
};

struct DropPhysicalSourceStatement {
    uint64_t id;
};

/// ShowPhysicalSourcesStatement, on the other hand, cannot reference a logical source by name that doesn't exist because it is directly
/// referencing a dms object
struct ShowPhysicalSourcesStatement {
    std::optional<LogicalSourceName> logicalSource;
    std::optional<uint32_t> id;
};

struct CreateSinkStatement
{
    Identifier name;
    Identifier sinkType;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    std::optional<Host> host;
    std::unordered_map<Identifier, std::string> sinkConfig;
    std::unordered_map<Identifier, std::string> formatConfig;
};

struct ShowSinksStatement
{
    std::optional<Identifier> name;
};

struct DropSinkStatement
{
    Identifier name;
};

struct QueryStatement {
    LogicalPlan plan;
    std::optional<Identifier> name;
};

struct ExplainQueryStatement
{
    LogicalPlan plan;
};

struct ShowQueriesStatement
{
    std::optional<uint64_t> id;
    std::optional<Identifier> name;
};

struct DropQueryStatement {
    std::optional<Identifier> name;
    std::optional<uint64_t> id;
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
    std::optional<Identifier> name;
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
    std::string hostAddr;
    std::string dataAddr;
    std::optional<size_t> maxOperators;
    std::vector<std::string> downstream;
    std::unordered_map<std::string, std::string> config; /// Flat dot-separated config map (e.g., "worker.receiver_queue_size" -> "2")
};

struct DropWorkerStatement {
    std::string host;
};

struct ShowWorkersStatement {
    std::optional<std::string> host;
};

using Statement = std::variant<
    CreateLogicalSourceStatement,
    ShowLogicalSourcesStatement,
    DropLogicalSourceStatement,
    CreatePhysicalSourceStatement,
    ShowPhysicalSourcesStatement,
    DropPhysicalSourceStatement,
    CreateSinkStatement,
    ShowSinksStatement,
    DropSinkStatement,
    QueryStatement,
    ExplainQueryStatement,
    ShowQueriesStatement,
    DropQueryStatement,
    WorkerStatusStatement,
    CreateWorkerStatement,
    DropWorkerStatement,
    ShowWorkersStatement,
    CreateModelStatement,
    ShowModelsStatement,
    DropModelStatement>;

class StatementBinder
{
    /// PIMPL pattern to hide all the internally used binder member functions
    class Impl;
    std::unique_ptr<Impl> impl;

public:
    explicit StatementBinder (
    const std::function<LogicalPlan(AntlrSQLParser::QueryContext *)> &queryPlanBinder
    );

    StatementBinder(const StatementBinder& other) = delete;
    StatementBinder& operator=(const StatementBinder& other) = delete;
    StatementBinder(StatementBinder&& other) noexcept;
    StatementBinder& operator=(StatementBinder&& other) noexcept;

    /// If the destructor was implicitly defaulted, it would call the destructor of the unique ptr, which would require the definition of Impl.
    /// Deferring the destructor default to the implementation, where also the definition of Impl is, solves this problem.
    ~StatementBinder();
    [[nodiscard]] std::expected<Statement, Exception> bind(AntlrSQLParser::StatementContext* statementAST) const;
    [[nodiscard]] std::expected<std::vector<std::expected<Statement, Exception>>, Exception>
    parseAndBind(std::string_view statementString) const;
    [[nodiscard]] std::expected<Statement, Exception> parseAndBindSingle(std::string_view statementStrings) const;
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
