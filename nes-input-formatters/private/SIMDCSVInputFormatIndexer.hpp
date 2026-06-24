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

/// SIMD-accelerated alternative to CSVInputFormatIndexer, based on the structural-index algorithm of
/// https://github.com/geofflangdale/simdcsv. It reuses CSVMetaData and the CSV descriptor config, selected
/// by setting the input formatter `type` to "SIMDCSV". The SIMD core is portable via runtime CPU
/// dispatch (SSE2/AVX2 on x86-64, NEON on arm64, scalar fallback elsewhere); see SIMDCSVKernel.hpp.
///
/// EXPERIMENT (REVERT/productionize before merge): this formatter is temporarily repurposed to the fast
/// flat-band path -- it emits raw structural positions into a FieldBand (branchless `indexBandInto`) instead
/// of grouped FieldOffsets, to measure the flat-band end-to-end win. See FieldBand.hpp. (The structured
/// SimdCsv::indexInto remains available for the scalar-equivalent path.)
class SIMDCSVInputFormatIndexer : public InputFormatIndexer<SIMDCSVInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SIMDCSV";
    static constexpr bool IsSequential = false;

    using IndexerMetaData = CSVMetaData;
    using FieldIndexFunctionType = FieldBand;

    explicit SIMDCSVInputFormatIndexer(const InputFormatterDescriptor& config);
    ~SIMDCSVInputFormatIndexer() = default;

    void indexRawBuffer(FieldBand& band, const RawTupleBuffer& rawBuffer, const CSVMetaData& metaData) const;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SIMDCSVInputFormatIndexer& obj);

private:
    bool allowCommasInStrings{};
    /// Block kernel resolved once for the running CPU; the indexer is constructed once per formatter
    /// instance and is otherwise stateless, keeping the hot indexRawBuffer path dispatch-free.
    SimdCsv::ComputeBlocksFn computeBlocks{};
};

}
