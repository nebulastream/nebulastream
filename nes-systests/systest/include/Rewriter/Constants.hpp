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

/// The SQL building blocks the rewriter emits.
/// These names are identifiers the binder interprets rather than grammar tokens, so ANTLR cannot supply them.
/// This file defines them once for the whole rewriter.
namespace NES::Sql
{

/// The config groups a statement addresses.
/// The rewriter quotes them, so a group whose name is also a keyword still reads as an identifier.
constexpr std::string_view Source = "SOURCE";
constexpr std::string_view Sink = "SINK";
constexpr std::string_view Query = "QUERY";
constexpr std::string_view InputFormatter = "INPUT_FORMATTER";

/// The config keys within those groups.
constexpr std::string_view FilePath = "FILE_PATH";
constexpr std::string_view Host = "HOST";
constexpr std::string_view OutputFormat = "OUTPUT_FORMAT";
constexpr std::string_view Type = "TYPE";
constexpr std::string_view Name = "NAME";
constexpr std::string_view Schema = "SCHEMA";

/// The format the rewriter writes when a sink or source chose none.
constexpr std::string_view Csv = "CSV";

/// The output format that writes one JSON object per row, which the checker reads whole rather than splitting into fields.
constexpr std::string_view Json = "JSON";

/// The sink type that discards its input.
/// The rewriter recognizes it because such a sink takes neither a file nor an output format, and leaves no result to check.
constexpr std::string_view Void = "Void";

/// The source type that reads from a socket.
/// The rewriter recognizes it because a server sends its attached data, and the port is known only once that server binds.
constexpr std::string_view Tcp = "TCP";

/// The endpoint keys such a source reads from, and how long it buffers before flushing.
constexpr std::string_view SocketHost = "SOCKET_HOST";
constexpr std::string_view SocketPort = "SOCKET_PORT";
constexpr std::string_view FlushIntervalMs = "FLUSH_INTERVAL_MS";

/// Returns the value as a SQL string literal.
/// A quote inside the value is doubled, so a path or a pattern holding one cannot end the literal early.
inline std::string stringLiteral(const std::string_view value)
{
    std::string escaped;
    escaped.reserve(value.size());
    for (const auto character : value)
    {
        escaped.push_back(character);
        if (character == '\'')
        {
            escaped.push_back(character);
        }
    }
    return fmt::format("'{}'", escaped);
}

/// Returns one config option, `'value' AS "group"."key"`.
/// Both names are quoted, so a name that is or becomes a keyword still reads as an identifier and no caller has to know which is which.
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
