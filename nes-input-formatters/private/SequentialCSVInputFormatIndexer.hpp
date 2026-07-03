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

#include <ostream>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <CSVInputFormatIndexer.hpp>
#include <FieldBand.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexer.hpp>
#include <SIMDCSVKernel.hpp>

namespace NES
{

/// Sequential (single-thread, in-order) CSV indexer for the `threading_mode=SEQUENTIAL` path, where the
/// blocking source runner runs index+parse INLINE on the source thread (cache-hot). Derived from the
/// SIMDCSV flat-band indexer: it reuses the same branchless SIMD structural scan and the FieldBand
/// representation, so the two-phase (index -> compiled parse) split is preserved. The only difference
/// from SIMDCSV is the leading-tuple handling: sequential buffers are ALIGNED (the runner prepends the
/// prior buffer's truncated bytes), so bytes [0 -> firstNewline] are a complete tuple 0, not a spanning
/// fragment. `SimdCsv::indexBandSequentialInto` writes a synthetic anchor at band[0] to express that;
/// there is no SequenceShredder (in-order delivery), spanning is handled by the sequential accumulator
/// in InputFormatter.
class SequentialCSVInputFormatIndexer : public InputFormatIndexer<SequentialCSVInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SequentialCSV";
    static constexpr bool IsSequential = true;

    using IndexerMetaData = CSVMetaData;
    using FieldIndexFunctionType = FieldBand;

    explicit SequentialCSVInputFormatIndexer(const InputFormatterDescriptor& config);
    ~SequentialCSVInputFormatIndexer() = default;

    void indexRawBuffer(FieldBand& band, const RawTupleBuffer& rawBuffer, const CSVMetaData& metaData) const;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SequentialCSVInputFormatIndexer& obj);

private:
    bool allowCommasInStrings{};
    /// Block kernel resolved once for the running CPU; the indexer is otherwise stateless, keeping the hot
    /// indexRawBuffer path dispatch-free.
    SimdCsv::ComputeBlocksFn computeBlocks{};
};

}
