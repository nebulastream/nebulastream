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

/// Interpreted (legacy) HL7 / packed-XML writer at the CSVFormat standard: hardcoded fast codecs
/// (std::to_chars integers, zmij shortest floats == ZMIJF64) and direct writes into the reused
/// caller buffer. The emission model mirrors the compiled HL7OutputFormatter byte-for-byte:
/// per record `prologue f1 <fieldDelimiter> f2 ... fn suffix`, NULL fields emit zero bytes (HL7's
/// `||` empty-field convention), varsized forwarded verbatim (no quoting/escaping).
///
/// Configuration comes as PRESETS keyed by the sink's `legacy_output_format` string -- the legacy
/// path has no config plumbing on purpose (the compiled HL7OutputFormatter carries the
/// config-generality claim; the interpreted baseline implements the two concrete wire formats the
/// evaluation uses):
///   "HL7": the compiled formatter's defaults -- prologue <VT> MSH-header <CR> SEG |, delimiter |,
///          suffix <CR><FS><CR>.
///   "XML": the packed-XML preset (see XMLOutput.test) -- prologue `<rec><f></f><f>` (arity-pad
///          element included), delimiter `</f><f>`, suffix `</f></rec>\n`.
class HL7Format : public Format
{
public:
    /// The three constant glue strings of the emission model (prologue = frame_start +
    /// message_header + segment_delimiter + segment_name + field_delimiter; suffix =
    /// segment_delimiter + frame_end -- pre-concatenated, mirroring the compiled formatter's
    /// fieldPrefixes/recordSuffix construction).
    struct Preset
    {
        std::string prologue;
        std::string fieldDelimiter;
        std::string suffix;
    };

    static Preset hl7Preset();
    static Preset xmlPreset();

    /// Precalculated per-field layout, built once at construction.
    struct FormattingContext
    {
        size_t schemaSizeInBytes{};
        std::vector<size_t> offsets;
        std::vector<size_t> sizesWithNull;
        std::vector<DataType> physicalTypes;
    };

    HL7Format(const Schema& schema, Preset preset);

    /// Convenience API: delegates to formatToBuffer, materializes a string only here.
    [[nodiscard]] std::string getFormattedBuffer(const TupleBuffer& inputBuffer) const override;

    [[nodiscard]] size_t formatToBuffer(const TupleBuffer& inputBuffer, std::vector<char>& out) const override;

    std::ostream& toString(std::ostream& os) const override { return os << *this; }

    friend std::ostream& operator<<(std::ostream& out, const HL7Format& format);

private:
    FormattingContext formattingContext;
    Preset preset;
};

}
