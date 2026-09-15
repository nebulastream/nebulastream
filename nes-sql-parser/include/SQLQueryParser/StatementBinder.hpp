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
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <AntlrSQLParser.h>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/LogicalSource.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/base.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// The target of a DROP or STOP statement: the entity that matches a filter, or all of them.
/// The grammar requires either WHERE or ALL, so a forgotten filter is a syntax error:
///     DROP SINK WHERE NAME = 'mySink'  -> Selection::of(MYSINK)
///     DROP SINK ALL                    -> Selection::all()
///     DROP SINK                        -> syntax error
template <typename Target>
class Selection
{
public:
    static Selection all() { return Selection{std::nullopt}; }

    static Selection of(Target target) { return Selection{std::optional<Target>{std::move(target)}}; }

    [[nodiscard]] bool selectsAll() const { return not target.has_value(); }

    /// Unset when every entity is selected.
    [[nodiscard]] const std::optional<Target>& getTarget() const { return target; }

    /// Maps the target and keeps a selection of every entity as it is.
    template <typename Function>
    [[nodiscard]] Selection<std::invoke_result_t<Function, const Target&>> transform(Function&& function) const
    {
        using Mapped = Selection<std::invoke_result_t<Function, const Target&>>;
        if (target.has_value())
        {
            return Mapped::of(std::invoke(std::forward<Function>(function), *target));
        }
        return Mapped::all();
    }

    bool operator==(const Selection&) const = default;

private:
    explicit Selection(std::optional<Target> target) : target{std::move(target)} { }

    std::optional<Target> target;
};

using LogicalSourceName = Identifier;

/// The name of the option group that holds the output formatter options of a sink.
inline constexpr std::string_view OUTPUT_FORMATTER_OPTION_GROUP{"OUTPUT_FORMATTER"};

/// The binder only parses the statements below.
/// The coordinator executes them against its catalog.
struct CreateLogicalSourceStatement
{
    Identifier name;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    bool ifNotExists = false;
};

/// ShowLogicalSourcesStatement only contains a name not bound to a logical statement,
/// because searching for a name for which no logical source exists is not a syntax error but just returns an empty result
struct ShowLogicalSourcesStatement
{
    std::optional<Identifier> name;
};

struct DropLogicalSourceStatement
{
    Selection<LogicalSourceName> source;
};

struct CreatePhysicalSourceStatement
{
    LogicalSourceName attachedTo;
    Identifier sourceType;
    std::optional<Host> host;
    std::unordered_map<Identifier, std::string> sourceConfig;
    std::unordered_map<Identifier, std::string> parserConfig;
    bool ifNotExists = false;
    friend std::ostream& operator<<(std::ostream& os, const CreatePhysicalSourceStatement& obj);
};

struct DropPhysicalSourceStatement
{
    Selection<PhysicalSourceId> id;
};

/// The logical source is only a name here.
/// The catalog decides whether it exists.
struct ShowPhysicalSourcesStatement
{
    std::optional<LogicalSourceName> logicalSource;
    std::optional<PhysicalSourceId> id;
};

struct CreateSinkStatement
{
    Identifier name;
    Identifier sinkType;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    std::optional<Host> host;
    std::unordered_map<Identifier, std::string> sinkConfig;
    std::unordered_map<Identifier, std::string> formatConfig;
    bool ifNotExists = false;
};

struct ShowSinksStatement
{
    std::optional<Identifier> name;
};

struct DropSinkStatement
{
    Selection<Identifier> name;
};

struct QueryStatement
{
    LogicalPlan plan;
    std::optional<Identifier> name;
    bool ifNotExists = false;
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
    std::optional<QueryId> id;
    std::optional<Identifier> name;
};

/// DROP QUERY and STOP QUERY select their queries by id, by name, or all at once.
using QuerySelection = Selection<std::variant<QueryId, Identifier>>;

struct DropQueryStatement
{
    QuerySelection queries;
};

struct StopQueryStatement
{
    QuerySelection queries;
};

struct CreateModelStatement
{
    std::string name;
    std::string path;
    Schema<UnqualifiedUnboundField, Ordered> inputs;
    Schema<UnqualifiedUnboundField, Ordered> outputs;
    bool ifNotExists = false;
};

struct ShowModelsStatement
{
    std::optional<Identifier> name;
};

struct ShowVersionStatement
{
    std::optional<std::string> host;
};

struct DropModelStatement
{
    Selection<std::string> name;
};

struct CreateWorkerStatement
{
    std::string host;
    std::string dataAddress;
    std::optional<size_t> maxOperators;
    std::vector<std::string> downstream;
    std::unordered_map<std::string, std::string> config; /// Flat dot-separated config map (e.g., "worker.receiver_queue_size" -> "2")
    bool ifNotExists = false;
};

struct DropWorkerStatement
{
    std::string host;
};

struct ShowWorkersStatement
{
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
    StopQueryStatement,
    CreateWorkerStatement,
    DropWorkerStatement,
    ShowWorkersStatement,
    CreateModelStatement,
    ShowModelsStatement,
    DropModelStatement,
    ShowVersionStatement>;

class StatementBinder
{
    /// PIMPL pattern to hide all the internally used binder member functions
    class Impl;
    std::unique_ptr<Impl> impl;

public:
    explicit StatementBinder(const std::function<LogicalPlan(AntlrSQLParser::QueryContext*)>& queryPlanBinder);

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
FMT_OSTREAM(NES::ExplainQueryStatement);
FMT_OSTREAM(NES::CreateWorkerStatement);
FMT_OSTREAM(NES::DropWorkerStatement);
