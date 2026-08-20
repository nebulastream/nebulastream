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

#include <Identifiers/Identifier.hpp>
#include <Util/Strings.hpp>

/// SQL building blocks that the rewriter emits.
/// The grammar does not export its keywords and config names, so they are repeated here and have to stay in sync with it.
namespace NES::Sql
{

/// Config groups
constexpr auto Source = "SOURCE";
constexpr auto Sink = "SINK";
constexpr auto InputFormatter = "INPUT_FORMATTER";
constexpr auto OutputFormatter = "OUTPUT_FORMATTER";

/// Config keys within the groups.
constexpr auto FilePath = "FILE_PATH";
constexpr auto Host = "HOST";
constexpr auto OutputFormat = "OUTPUT_FORMAT";
constexpr auto Type = "TYPE";
constexpr auto Schema = "SCHEMA";
constexpr auto QuoteStrings = "QUOTE_STRINGS";

/// Default input/output format
constexpr auto Csv = "CSV";

/// Default sink type.
constexpr auto File = "File";
/// Sink that discards its input and takes no file path.
constexpr auto Void = "Void";
/// Sink types that the engine accepts but that write nothing a test can read.
constexpr auto Print = "Print";
constexpr auto Mqtt = "MQTT";
/// Sink type that writes a checksum over its rows instead of the rows.
constexpr auto Checksum = "CHECKSUM";
constexpr auto Tcp = "TCP";
/// Config keys for a TCP source.
constexpr auto SocketHost = "SOCKET_HOST";
constexpr auto SocketPort = "SOCKET_PORT";
constexpr auto FlushIntervalMs = "FLUSH_INTERVAL_MS";

/// Returns whether two spellings resolve to the same name under the binder's case rules.
/// A quoted and an unquoted spelling of one name match, and two unquoted spellings differing only in case also match.
inline bool sameName(const std::string_view left, const std::string_view right)
{
    return Identifier::parse(std::string{left}) == Identifier::parse(std::string{right});
}

/// Only `File` and `Checksum` take a file path, so only they can be pointed at the file that the checker reads.
inline bool writesReadableResult(const std::string_view sinkType)
{
    return sameName(sinkType, File) or sameName(sinkType, Checksum);
}

/// Returns the value as a SQL string literal.
/// A quote inside the value is doubled, so a path or a pattern holding one cannot end the literal early.
inline std::string stringLiteral(const std::string_view value)
{
    return fmt::format("'{}'", replaceAll(value, "'", "''"));
}

/// Returns one config option, `'value' AS "group"."key"`.
/// Both names are quoted, so the parser treats a name that is or becomes a keyword as an identifier and no caller has to distinguish.
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

inline std::string optionList(const std::vector<std::string>& options)
{
    return fmt::to_string(fmt::join(options, ", "));
}

inline std::string setClause(const std::string_view body)
{
    return fmt::format("SET ({})", body);
}

inline std::string sink(const std::string_view type, const std::string_view body)
{
    return fmt::format("{}({})", type, body);
}

}
