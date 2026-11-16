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

/// Generic config groups
constexpr auto Source = "SOURCE";
constexpr auto Sink = "SINK";
constexpr auto InputFormatter = "INPUT_FORMATTER";
constexpr auto OutputFormatter = "OUTPUT_FORMATTER";

/// Per-plugin config groups
constexpr auto FileSourceGroup = "FILE_SOURCE";
constexpr auto TcpSourceGroup = "TCP_SOURCE";
constexpr auto FileSinkGroup = "FILE_SINK";
constexpr auto ChecksumSinkGroup = "CHECKSUM_SINK";
constexpr auto CsvOutputFormatterGroup = "CSV_OUTPUT_FORMATTER";

/// Config keys within the groups.
constexpr auto FilePath = "FILE_PATH";
constexpr auto Host = "HOST";
constexpr auto Type = "TYPE";
constexpr auto Schema = "SCHEMA";
constexpr auto QuoteStrings = "QUOTE_STRINGS";
constexpr auto Path = "PATH";

/// Default input/output format
constexpr auto Csv = "CSV";

/// Source & sink plugin names.
constexpr auto FileSource = "FileSource";
constexpr auto TcpSource = "TCPSource";
constexpr auto FileSink = "FileSink";
/// Sink that discards its input and takes no file path.
constexpr auto VoidSink = "VoidSink";
/// Sink types that the engine accepts but that write nothing a test can read.
constexpr auto PrintSink = "PrintSink";
constexpr auto MqttSink = "MQTTSink";
/// Sink type that writes a checksum over its rows instead of the rows.
constexpr auto ChecksumSink = "ChecksumSink";

/// Table-valued operator & model functor names.
constexpr auto ModelInference = "MODEL_INFERENCE";
constexpr auto OpenVinoModel = "OPENVINO_MODEL";

/// Config keys for a TCP source.
constexpr auto SocketHost = "SOCKET_HOST";
constexpr auto SocketPort = "SOCKET_PORT";
constexpr auto FlushIntervalMs = "FLUSH_INTERVAL_MS";

/// Returns whether two spellings resolve to the same name under the binder's case rules.
/// A quoted and an unquoted spelling of one name match, and two unquoted spellings differing only in case also match.
inline bool sameName(const std::string_view left, const std::string_view right)
{
    const auto parsedLeft = Identifier::tryParse(std::string{left});
    const auto parsedRight = Identifier::tryParse(std::string{right});
    return parsedLeft.has_value() and parsedRight.has_value() and *parsedLeft == *parsedRight;
}

/// Only `FileSink` and `ChecksumSink` take a file path, so only they can be pointed at the file that the checker reads.
inline bool writesReadableResult(const std::string_view sinkType)
{
    return sameName(sinkType, FileSink) or sameName(sinkType, ChecksumSink);
}

/// Returns the per-plugin config group that holds `FILE_PATH` for a readable sink type.
inline std::string_view sinkFilePathGroup(const std::string_view sinkType)
{
    return sameName(sinkType, ChecksumSink) ? ChecksumSinkGroup : FileSinkGroup;
}

/// Returns the value as a SQL string literal.
/// A quote inside the value is doubled, so a path or a pattern holding one cannot end the literal early.
inline std::string stringLiteral(const std::string_view value)
{
    return fmt::format("'{}'", replaceAll(value, "'", "''"));
}

/// Returns a config option from an already formatted SQL literal.
inline std::string literalOption(const std::string_view group, const std::string_view key, const std::string_view literal)
{
    return fmt::format(R"({} AS "{}"."{}")", literal, group, key);
}

/// Returns one config option, `'value' AS "group"."key"`.
/// Both names are quoted, so the parser treats a name that is or becomes a keyword as an identifier and no caller has to distinguish.
inline std::string option(const std::string_view group, const std::string_view key, const std::string_view value)
{
    return literalOption(group, key, stringLiteral(value));
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
