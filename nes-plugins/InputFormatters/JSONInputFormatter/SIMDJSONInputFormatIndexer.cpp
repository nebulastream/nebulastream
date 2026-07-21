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

#include <SIMDJSONInputFormatIndexer.hpp>

#include <expected>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/Strings.hpp>
#include <Util/Variant.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>
#include <SIMDJSONRawBufferIndex.hpp>

namespace NES
{

std::unique_ptr<RawBufferIndex> SIMDJSONInputFormatIndexer::indexRawBuffer(const std::string_view rawBuffer) const
{
    auto rawBufferIndex = std::make_unique<SIMDJSONRawBufferIndex>();

    const auto offsetOfFirstTuple = static_cast<FieldIndex>(rawBuffer.find(TUPLE_DELIMITER));

    /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTuple' to a value larger than the buffer size to tell
    /// the InputFormatter that there was no tuple delimiter in the buffer and return
    if (offsetOfFirstTuple == static_cast<FieldIndex>(std::string::npos))
    {
        rawBufferIndex->markNoTupleDelimiters();
        return rawBufferIndex;
    }

    /// If the buffer contains at least one delimiter, check if it contains more and index all tuples between the tuple delimiters
    const auto startIdxOfNextTuple = offsetOfFirstTuple + DELIMITER_SIZE;

    const auto jsonSV = rawBuffer.substr(startIdxOfNextTuple);
    const auto [isNoTuple, truncatedBytes] = rawBufferIndex->indexJSON(jsonSV);
    const auto offsetOfLastTuple = static_cast<FieldIndex>(
        (isNoTuple) ? offsetOfFirstTuple : rawBuffer.size() - truncatedBytes - this->getTupleDelimitingBytes().size());

    rawBufferIndex->markWithTupleDelimiters(offsetOfFirstTuple, offsetOfLastTuple);
    return rawBufferIndex;
}

std::ostream& SIMDJSONInputFormatIndexer::toString(std::ostream& str) const
{
    return str << fmt::format("SIMDJSONInputFormatIndexer(tupleDelimiter: {})", SIMDJSONInputFormatIndexer::TUPLE_DELIMITER);
}

namespace
{

/// Config fields of the JSON input formatter, shared by getConfigSchema (declaration) and
/// SIMDJSONInputFormatterConfig::fromConfig (typed extraction).
/// NOLINTBEGIN(cert-err58-cpp)
static const ConfigField<char> TUPLE_DELIMITER_FIELD{
    "TUPLE_DELIMITER",
    /// Single-byte delimiter parameter; unescapes textual escape sequences such as "\n" or "\t" first.
    [](const ConfigLiteral& literal)
    {
        return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](const std::string& value) -> std::expected<char, Exception>
                {
                    const auto unescaped = unescapeSpecialCharacters(value);
                    if (unescaped.size() != 1)
                    {
                        return std::unexpected{InvalidConfigParameter("Expected a single (possibly escaped) character, got {}", value)};
                    }
                    return unescaped.front();
                });
    },
    '\n'};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> SIMDJSONInputFormatIndexer::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("JSON_INPUT_FORMATTER"), TUPLE_DELIMITER_FIELD);
}

std::expected<SIMDJSONInputFormatterConfig, Exception> SIMDJSONInputFormatterConfig::fromConfig(const InstantiatedConfig& config)
{
    return SIMDJSONInputFormatterConfig{.tupleDelimiter = config.get(TUPLE_DELIMITER_FIELD)};
}
}
