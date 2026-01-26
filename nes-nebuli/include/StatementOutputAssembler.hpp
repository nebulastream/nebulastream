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
#include <array>
#include <concepts>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <magic_enum/magic_enum.hpp>
#include <StatementHandler.hpp>

namespace NES
{

template <typename T>
struct StatementOutputAssembler;

namespace detail
{
/// A StatementOutputAssembler has a `.convert(Result)` function which converts the internal result representation into a named tuples.
/// Return type of the .convert() function
/// Assuming the result type of the conversion is a std::pair<TupleLike, Range<TupleLike>>
template <typename Result>
using ConversionType = std::invoke_result_t<decltype(&StatementOutputAssembler<Result>::convert), StatementOutputAssembler<Result>, Result>;
template <typename Result>
using ConversionNamesType = decltype(ConversionType<Result>::first);
template <typename Result>
using ConversionResultType = std::ranges::range_value_t<decltype(ConversionType<Result>::second)>;
}

template <typename Result>
concept AssemblembleStatementResult =
    /// StatementOutputAssembler specialization
    std::is_default_constructible_v<StatementOutputAssembler<Result>>
    /// There must be a name for every type result field
    && std::tuple_size_v<detail::ConversionNamesType<Result>> == std::tuple_size_v<detail::ConversionResultType<Result>>
    /// OutputAssembler convert return type and the advertised OutputRowType match
    && std::convertible_to<detail::ConversionResultType<Result>, typename StatementOutputAssembler<Result>::OutputRowType>;

using LogicalSourceOutputRowType = std::tuple<std::string, Schema>;
constexpr std::array<std::string_view, 2> logicalSourceOutputColumns{"source_name", "schema"};

using SourceDescriptorOutputRowType
    = std::tuple<PhysicalSourceId, std::string, Schema, std::string, ParserConfig, NES::DescriptorConfig::Config>;
constexpr std::array<std::string_view, 6> sourceDescriptorOutputColumns{
    "physical_source_id", "source_name", "schema", "source_type", "parser_config", "source_config"};

using SinkDescriptorOutputRowType = std::tuple<std::string, Schema, std::string, NES::DescriptorConfig::Config>;
constexpr std::array<std::string_view, 4> sinkDescriptorOutputColumns{"sink_name", "schema", "sink_type", "sink_config"};

using QueryIdOutputRowType = std::tuple<QueryId>;
constexpr std::array<std::string_view, 1> queryIdOutputColumns{"query_id"};

using QueryStatusOutputRowType = std::tuple<QueryId, std::string>;
constexpr std::array<std::string_view, 2> queryStatusOutputColumns{"query_id", "query_status"};

/// NOLINTBEGIN(readability-convert-member-functions-to-static)
template <>
struct StatementOutputAssembler<CreateLogicalSourceStatementResult>
{
    using OutputRowType = LogicalSourceOutputRowType;

    auto convert(const CreateLogicalSourceStatementResult& result)
    {
        return std::make_pair(
            logicalSourceOutputColumns, std::vector{std::make_tuple(result.created.getLogicalSourceName(), *result.created.getSchema())});
    }
};

template <>
struct StatementOutputAssembler<CreatePhysicalSourceStatementResult>
{
    using OutputRowType = SourceDescriptorOutputRowType;

    auto convert(const CreatePhysicalSourceStatementResult& result)
    {
        return std::make_pair(
            sourceDescriptorOutputColumns,
            std::vector{std::make_tuple(
                result.created.getPhysicalSourceId(),
                result.created.getLogicalSource().getLogicalSourceName(),
                *result.created.getLogicalSource().getSchema(),
                result.created.getSourceType(),
                result.created.getParserConfig(),
                result.created.getConfig())});
    }
};

template <>
struct StatementOutputAssembler<CreateSinkStatementResult>
{
    using OutputRowType = SinkDescriptorOutputRowType;

    auto convert(const CreateSinkStatementResult& result)
    {
        return std::make_pair(
            sinkDescriptorOutputColumns,
            std::vector{std::make_tuple(
                result.created.getSinkName(), *result.created.getSchema(), result.created.getSinkType(), result.created.getConfig())});
    }
};

template <>
struct StatementOutputAssembler<ShowLogicalSourcesStatementResult>
{
    using OutputRowType = LogicalSourceOutputRowType;

    auto convert(const ShowLogicalSourcesStatementResult& result)
    {
        std::vector<OutputRowType> output;
        output.reserve(result.sources.size());
        for (const auto& source : result.sources)
        {
            output.emplace_back(source.getLogicalSourceName(), *source.getSchema());
        }
        return std::make_pair(logicalSourceOutputColumns, output);
    }
};

template <>
struct StatementOutputAssembler<ShowPhysicalSourcesStatementResult>
{
    using OutputRowType = SourceDescriptorOutputRowType;

    auto convert(const ShowPhysicalSourcesStatementResult& result)
    {
        std::vector<OutputRowType> output;
        output.reserve(result.sources.size());
        for (const auto& source : result.sources)
        {
            output.emplace_back(
                source.getPhysicalSourceId(),
                source.getLogicalSource().getLogicalSourceName(),
                *source.getLogicalSource().getSchema(),
                source.getSourceType(),
                source.getParserConfig(),
                source.getConfig());
        }
        return std::make_pair(sourceDescriptorOutputColumns, output);
    }
};

template <>
struct StatementOutputAssembler<ShowSinksStatementResult>
{
    using OutputRowType = SinkDescriptorOutputRowType;

    auto convert(const ShowSinksStatementResult& result)
    {
        std::vector<OutputRowType> output;
        output.reserve(result.sinks.size());
        for (const auto& sink : result.sinks)
        {
            output.emplace_back(sink.getSinkName(), *sink.getSchema(), sink.getSinkType(), sink.getConfig());
        }
        return std::make_pair(sinkDescriptorOutputColumns, output);
    }
};

template <>
struct StatementOutputAssembler<DropLogicalSourceStatementResult>
{
    using OutputRowType = LogicalSourceOutputRowType;

    auto convert(const DropLogicalSourceStatementResult& result)
    {
        return std::make_pair(
            logicalSourceOutputColumns, std::vector{std::make_tuple(result.dropped.getLogicalSourceName(), *result.dropped.getSchema())});
    }
};

template <>
struct StatementOutputAssembler<DropPhysicalSourceStatementResult>
{
    using OutputRowType = SourceDescriptorOutputRowType;

    auto convert(const DropPhysicalSourceStatementResult& result)
    {
        return std::make_pair(
            sourceDescriptorOutputColumns,
            std::vector{std::make_tuple(
                result.dropped.getPhysicalSourceId(),
                result.dropped.getLogicalSource().getLogicalSourceName(),
                *result.dropped.getLogicalSource().getSchema(),
                result.dropped.getSourceType(),
                result.dropped.getParserConfig(),
                result.dropped.getConfig())});
    }
};

template <>
struct StatementOutputAssembler<DropSinkStatementResult>
{
    using OutputRowType = SinkDescriptorOutputRowType;

    auto convert(const DropSinkStatementResult& result)
    {
        return std::make_pair(
            sinkDescriptorOutputColumns,
            std::vector{std::make_tuple(
                result.dropped.getSinkName(), *result.dropped.getSchema(), result.dropped.getSinkType(), result.dropped.getConfig())});
    }
};

template <>
struct StatementOutputAssembler<QueryStatementResult>
{
    using OutputRowType = QueryIdOutputRowType;

    auto convert(const QueryStatementResult& result)
    {
        return std::make_pair(queryIdOutputColumns, std::vector{std::make_tuple(result.id)});
    }
};

template <>
struct StatementOutputAssembler<ShowQueriesStatementResult>
{
    using OutputRowType = QueryStatusOutputRowType;

    auto convert(const ShowQueriesStatementResult& result)
    {
        std::vector<OutputRowType> output;
        output.reserve(result.queries.size());
        for (const auto& [id, query] : result.queries)
        {
            output.emplace_back(id, magic_enum::enum_name(query.state));
        }
        return std::make_pair(queryStatusOutputColumns, output);
    }
};

template <>
struct StatementOutputAssembler<DropQueryStatementResult>
{
    using OutputRowType = QueryIdOutputRowType;

    auto convert(const DropQueryStatementResult& result)
    {
        std::vector<OutputRowType> output;
        output.reserve(result.ids.size());
        for (const auto& id : result.ids)
        {
            output.emplace_back(id);
        }
        return std::make_pair(queryIdOutputColumns, std::move(output));
    }
};

/// NOLINTEND(readability-convert-member-functions-to-static)


static_assert(AssemblembleStatementResult<CreateLogicalSourceStatementResult>);
static_assert(AssemblembleStatementResult<CreatePhysicalSourceStatementResult>);
static_assert(AssemblembleStatementResult<CreateSinkStatementResult>);
static_assert(AssemblembleStatementResult<ShowLogicalSourcesStatementResult>);
static_assert(AssemblembleStatementResult<ShowPhysicalSourcesStatementResult>);
static_assert(AssemblembleStatementResult<ShowSinksStatementResult>);
static_assert(AssemblembleStatementResult<DropLogicalSourceStatementResult>);
static_assert(AssemblembleStatementResult<DropPhysicalSourceStatementResult>);
static_assert(AssemblembleStatementResult<DropSinkStatementResult>);
static_assert(AssemblembleStatementResult<QueryStatementResult>);
static_assert(AssemblembleStatementResult<ShowQueriesStatementResult>);
static_assert(AssemblembleStatementResult<DropQueryStatementResult>);

}
