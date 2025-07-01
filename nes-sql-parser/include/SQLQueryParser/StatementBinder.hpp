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
#include <variant>

#include <Plans/LogicalPlan.hpp>

#include <AntlrSQLParser.h>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include "Configurations/Descriptor.hpp"
#include "Sinks/SinkDescriptor.hpp"

namespace NES
{

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
    LogicalSource attachedTo;
    std::string sourceType;
    std::unordered_map<std::string, std::string> sourceConfig;
    ParserConfig parserConfig;
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
    StatementOutputFormat format;
};

/// ShowPhysicalSourcesStatement, on the other hand, cannot reference a logical source by name that doesn't exist because it is directly
/// referencing a dms object
struct ShowPhysicalSourcesStatement
{
    std::optional<LogicalSource> logicalSource;
    std::optional<uint32_t> id;
    StatementOutputFormat format;
};

struct ShowSinksStatement
{
    std::optional<std::string> name;
    StatementOutputFormat format;
};


struct DropLogicalSourceStatement
{
    LogicalSource source;
};

struct DropPhysicalSourceStatement
{
    SourceDescriptor descriptor;
};

struct DropSinkStatement
{
    Sinks::SinkDescriptor descriptor;
};


using QueryStatement = LogicalPlan;

struct ShowQueriesStatement
{
    std::optional<QueryId> id;
    StatementOutputFormat format;
};

struct DropQueryStatement
{
    QueryId id;
};


using Statement = std::variant<
    CreateLogicalSourceStatement,
    CreatePhysicalSourceStatement,
    CreateSinkStatement,
    ShowLogicalSourcesStatement,
    ShowPhysicalSourcesStatement,
    DropLogicalSourceStatement,
    DropPhysicalSourceStatement,
    DropSinkStatement,
    QueryStatement,
    ShowQueriesStatement,
    ShowSinksStatement,
    DropQueryStatement>;


class StatementBinder
{
    /// PIMPL pattern to hide all the internally used binder member functions
    class Impl;
    std::unique_ptr<Impl> impl;

public:
    explicit StatementBinder(
        const std::shared_ptr<const SourceCatalog>& sourceCatalog,
        const std::shared_ptr<const SinkCatalog>& sinkCatalog,
        const std::function<LogicalPlan(AntlrSQLParser::QueryContext*)>& queryPlanBinder) noexcept;

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
    constexpr auto parse(const format_parse_context& ctx) const noexcept -> decltype(ctx.begin()) { return ctx.begin(); }

    constexpr auto format(const std::unordered_map<std::string, std::string>& mapToFormat, format_context& ctx) const noexcept
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
