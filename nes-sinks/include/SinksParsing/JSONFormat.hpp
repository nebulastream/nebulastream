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
#include <SinksParsing/Format.hpp>

#include <cstddef>
#include <ostream>
#include <string>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// Interpreted (legacy) JSON writer at the CSVFormat standard: hardcoded fast codecs
/// (std::to_chars integers, zmij shortest floats == ZMIJF64) and direct writes into the reused
/// caller buffer -- no per-field std::string, no fmt format-string parsing, no stringstream.
/// The wire format mirrors the compiled JSONOutputFormatter byte-for-byte on the conventions:
/// per-field glue `{"NAME":` / `,"NAME":` (names JSON-escaped at construction), lowercase
/// `null`, RFC 8259 string escaping, record suffix `}\n`. What stays interpreted is the
/// per-field schema-driven dispatch loop -- no fusion, no specialization, no lazy access.
class JSONFormat : public Format
{
public:
    /// Precalculated per-field layout + the constant glue strings, built once at construction.
    struct FormattingContext
    {
        size_t schemaSizeInBytes{};
        std::vector<size_t> offsets;
        std::vector<size_t> sizesWithNull;
        std::vector<std::string> prefixes;
        std::vector<DataType> physicalTypes;
        /// Raw (unescaped) field names -- only the Original (stringstream) writer consumes these.
        std::vector<std::string> names;
    };

    explicit JSONFormat(const Schema& schema);

    /// Convenience API: delegates to formatToBuffer, materializes a string only here.
    [[nodiscard]] std::string getFormattedBuffer(const TupleBuffer& inputBuffer) const override;

    [[nodiscard]] size_t formatToBuffer(const TupleBuffer& inputBuffer, std::vector<char>& out) const override;

    std::ostream& toString(std::ostream& os) const override { return os << *this; }

    friend std::ostream& operator<<(std::ostream& out, const JSONFormat& format);

private:
    /// The genuine pre-optimization naive writer (std::stringstream + DataType::formattedBytesToString),
    /// reachable only when NES_OUTPUT_CODEC=original. Kept verbatim as the naive output-serializer baseline
    /// (structure + codec together); the fast direct-buffer path is the optimized target.
    [[nodiscard]] std::string formatOriginalStringstream(const TupleBuffer& inputBuffer) const;

    FormattingContext formattingContext;
    OutputCodec codec;
};

}
