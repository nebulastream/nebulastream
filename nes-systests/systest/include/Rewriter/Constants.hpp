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

#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Util/Strings.hpp>

/// The SQL building blocks the rewriter emits.
/// These names are identifiers the binder interprets rather than grammar tokens, so ANTLR cannot supply them.
/// This file defines them once for the whole rewriter.
namespace NES::Sql
{

/// Config groups a statement addresses.
/// The rewriter quotes them, so a group whose name is also a keyword is treated as an identifier.
constexpr auto Source = "SOURCE";
constexpr auto Sink = "SINK";
constexpr auto Query = "QUERY";
constexpr auto InputFormatter = "INPUT_FORMATTER";

/// Config keys within the groups.
constexpr auto FilePath = "FILE_PATH";
constexpr auto Host = "HOST";
constexpr auto OutputFormat = "OUTPUT_FORMAT";
constexpr auto Type = "TYPE";
constexpr auto Name = "NAME";
constexpr auto Schema = "SCHEMA";

/// Format that the rewriter uses when a sink or source chose none.
constexpr auto Csv = "CSV";

/// Output format that writes one JSON object per row.
constexpr auto Json = "JSON";

/// Sink type that discards its input.
/// The rewriter recognizes it because such a sink takes no arguments, and produces no result to check.
constexpr auto Void = "Void";

/// The rewriter recognizes it because a server sends its attached data, and the port is known only once the server binds.
constexpr auto Tcp = "TCP";

/// The endpoint keys that such a source reads from, and how long it buffers before flushing.
constexpr auto SocketHost = "SOCKET_HOST";
constexpr auto SocketPort = "SOCKET_PORT";
constexpr auto FlushIntervalMs = "FLUSH_INTERVAL_MS";

/// Returns the value as a SQL string literal.
/// A quote inside the value is doubled, so a path or a pattern holding one cannot end the literal early.
inline std::string stringLiteral(const std::string_view value)
{
    return fmt::format("'{}'", replaceAll(value, "'", "''"));
}

/// Returns one config option, `'value' AS "group"."key"`.
/// Both names are quoted, so a name that is or becomes a keyword is treated as an identifier
/// and no caller has to distinguish.
inline std::string option(const std::string_view group, const std::string_view key, const std::string_view value)
{
    return fmt::format(R"({} AS "{}"."{}")", stringLiteral(value), group, key);
}

/// Returns a config option whose value is a schema literal rather than a quoted string, `SCHEMA<body> AS "group"."SCHEMA"`.
/// The leading `SCHEMA` is the schema-literal keyword, and the trailing quoted `SCHEMA` is the config key.
inline std::string schemaOption(const std::string_view group, const std::string_view schemaBody)
{
    return fmt::format(R"({}{} AS "{}"."{}")", Schema, schemaBody, group, Schema);
}

/// Joins config options into the body of a `SET` clause or a parameter list.
inline std::string optionList(const std::vector<std::string>& options)
{
    return fmt::to_string(fmt::join(options, ", "));
}

/// Wraps an option body in the `SET` clause that holds a statement's config.
inline std::string setClause(const std::string_view body)
{
    return fmt::format("SET ({})", body);
}

/// Wraps an option body in an anonymous sink of the given type, such as File or Checksum.
inline std::string sink(const std::string_view type, const std::string_view body)
{
    return fmt::format("{}({})", type, body);
}

}
