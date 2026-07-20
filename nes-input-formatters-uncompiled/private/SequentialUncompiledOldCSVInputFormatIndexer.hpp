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
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <DataTypes/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <UncompiledInputFormatters/UncompiledInputFormatterTaskPipeline.hpp>
#include <UncompiledFieldOffsets.hpp>
#include <UncompiledInputFormatIndexer.hpp>

namespace NES
{

/// Deliberately does NOT reuse ConfigParametersUncompiledCSVInputFormatIndexer: that struct defaults
/// `allow_commas_in_strings` to TRUE, and including its header would drag SIMDCSVKernel.hpp +
/// UncompiledFieldBand.hpp into a plugin that touches neither.
struct ConfigParametersSequentialUncompiledOldCSVInputFormatIndexer
{
    /// Defaults FALSE, in lockstep with the compiled and uncompiled band indexers. This indexer is the
    /// ablation's rung-1 floor, so its default has to be the same SEMANTIC work the rungs above it are
    /// measured at -- the quote scan is a separate lever worth ~-27% index CPU / +13.5% e2e (the no-quote
    /// SIMD kernel, c489135d05), and bundling it into the rung-1->rung-2 step would credit the SIMD band
    /// with a saving that is really the quote lever's.
    static inline const DescriptorConfig::ConfigParameter<bool> ALLOW_COMMAS_IN_STRINGS{
        "allow_commas_in_strings",
        false,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(ALLOW_COMMAS_IN_STRINGS, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> TUPLE_DELIMITER{
        "tuple_delimiter",
        "\n",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TUPLE_DELIMITER, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> FIELD_DELIMITER{
        "field_delimiter",
        ",",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FIELD_DELIMITER, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            InputFormatterDescriptor::parameterMap, ALLOW_COMMAS_IN_STRINGS, TUPLE_DELIMITER, FIELD_DELIMITER);
};

struct SequentialUncompiledOldCSVMetaData
{
    explicit SequentialUncompiledOldCSVMetaData(const InputFormatterDescriptor& config, const Schema&)
        : tupleDelimiter(config.getFromConfig(ConfigParametersSequentialUncompiledOldCSVInputFormatIndexer::TUPLE_DELIMITER)) { };

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }

private:
    std::string tupleDelimiter;
};

/// The ABLATION BASELINE: NebulaStream's original CSV indexer, kept alive as its own plugin.
///
/// Algorithm-identical to main's `CSVInputFormatIndexer` -- scalar `std::string_view::find` per tuple
/// delimiter, then per field delimiter, emitting one offset per field into `UncompiledFieldOffsets`,
/// with main's two per-tuple variants (plain / quote-aware) selected by `allow_commas_in_strings` --
/// and interpreted end to end: no SIMD, no JIT. It is the rung-1 floor the rest of the ladder is
/// measured against. Rung 2 (`SequentialUncompiledCSV`) swaps this scalar scan for the branchless SIMD
/// flat band while staying interpreted, isolating the INDEXING kernel; rung 3 (`SequentialCSV`) then
/// adds compilation on top of the same band.
///
/// The PARSE path is shared, not reimplemented: `processUncompiledTuple` is generic over the field
/// index function and dispatches directly to the hardcoded fast codecs (`uncompiledFastParse` =
/// fast_float), so this rung differs from rung 2 in the INDEXER only -- exactly the intent.
///
/// Sequential only, by design. The parallel scalar path is what `UncompiledCSV` already was before
/// 5687bb7e00 migrated it to the band, and the ladder never needs it: parallelism is a later rung.
/// `IsSequential = true` is not cosmetic -- `UncompiledInputFormatterTask` keys `if constexpr` on it to
/// drop the `UncompiledSequenceShredder` member entirely (`[[no_unique_address]]` + `conditional_t`)
/// and swap in the single-threaded `SequentialCarry` accumulator, so no parallel machinery survives.
///
/// Buffer contract, shared with the band-based sequential indexers: the leading partial row
/// [0 -> firstTupleDelimiter) is NOT indexed -- the sequential task glue completes it from its carry
/// accumulator -- the bulk between the first and last delimiter is indexed in place, and the trailing
/// partial is carried to the next buffer.
class SequentialUncompiledOldCSVInputFormatIndexer : public UncompiledInputFormatIndexer<SequentialUncompiledOldCSVInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SequentialUncompiledOldCSV";
    static constexpr bool IsFormattingRequired = true;
    static constexpr bool HasSpanningTuple = true;
    static constexpr bool IsSequential = true;
    using UncompiledIndexerMetaData = SequentialUncompiledOldCSVMetaData;
    using UncompiledFieldIndexFunctionType = UncompiledFieldOffsets<UncompiledNumRequiredOffsetsPerField::ONE>;

    explicit SequentialUncompiledOldCSVInputFormatIndexer(InputFormatterDescriptor config, size_t numberOfFieldsInSchema);
    ~SequentialUncompiledOldCSVInputFormatIndexer() = default;

    void indexRawBuffer(
        UncompiledFieldOffsets<UncompiledNumRequiredOffsetsPerField::ONE>& fieldOffsets,
        const UncompiledRawTupleBuffer& rawBuffer,
        const SequentialUncompiledOldCSVMetaData&) const;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SequentialUncompiledOldCSVInputFormatIndexer& obj);

private:
    char tupleDelimiter;
    char fieldDelimiter;
    size_t numberOfFieldsInSchema;
    /// Selects main's quote-aware per-tuple scan over the plain one. Defaults FALSE here (see the config
    /// struct); honored rather than ignored, so a quoted-CSV source cannot silently mis-index.
    bool allowCommasInStrings;
};

}
