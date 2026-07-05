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

#include <SIMDCSVInputFormatIndexer.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <fmt/format.h>
#include <CSVInputFormatIndexer.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatter.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <InputFormatterValidationRegistry.hpp>
#include <RawTupleBuffer.hpp>
#include <SIMDCSVKernel.hpp>
#include <SIMDCSVScan.hpp>

namespace
{
/// TMP DIAGNOSTIC: per-buffer wall time in SIMDCSVInputFormatIndexer::indexRawBuffer, summed across
/// threads, printed at process exit -- the SIMD counterpart of DIAG_CSVINDEX, for a direct scalar-vs-SIMD
/// index-CPU comparison. Per-buffer (cheap), so a plain atomic is fine here. REVERT before merge.
std::atomic<std::uint64_t> g_simdIdxNs{0};
std::atomic<std::uint64_t> g_simdIdxCalls{0};

const struct SimdIdxDiagPrinter
{
    ~SimdIdxDiagPrinter()
    {
        std::fprintf(
            stderr,
            "DIAG_SIMDINDEX index_cpu=%.4fs calls=%llu\n",
            g_simdIdxNs.load() / 1e9,
            static_cast<unsigned long long>(g_simdIdxCalls.load()));
    }
} g_simdIdxDiagPrinter;
}

namespace NES
{

SIMDCSVInputFormatIndexer::SIMDCSVInputFormatIndexer(const InputFormatterDescriptor& config)
    : allowCommasInStrings(config.getFromConfig(ConfigParametersCSVInputFormatIndexer::ALLOW_COMMAS_IN_STRINGS))
    /// When quotes are not honored, pick the no-quote kernel: it skips the quote SIMD compare the driver
    /// would otherwise never read (member init order: allowCommasInStrings is declared first, so it is set).
    , computeBlocks(SimdCsv::selectComputeBlocks(allowCommasInStrings))
{
}

void SIMDCSVInputFormatIndexer::indexRawBuffer(FieldBand& band, const RawTupleBuffer& rawBuffer, const CSVMetaData& metaData) const
{
    const struct ScopeTimer
    {
        std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();

        ~ScopeTimer()
        {
            g_simdIdxNs.fetch_add(
                static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count()),
                std::memory_order_relaxed);
            g_simdIdxCalls.fetch_add(1, std::memory_order_relaxed);
        }
    } scopeTimer;

    /// EXPERIMENT: fast flat-band path (raw structural positions) instead of structured FieldOffsets.
    SimdCsv::indexBandInto(
        band,
        rawBuffer.getBufferView(),
        metaData.getFieldDelimiter(),
        metaData.getTupleDelimiter(),
        allowCommasInStrings,
        metaData.getNumberOfFields(),
        computeBlocks);
}

DescriptorConfig::Config SIMDCSVInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCSVInputFormatIndexer>(std::move(config), NAME);
}

InputFormatIndexerRegistryReturnType
RegisterSIMDCSVInputFormatIndexer(InputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createInputFormatterWithIndexer(SIMDCSVInputFormatIndexer{arguments.getInputFormatterConfig()});
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterSIMDCSVInputFormatterValidation(InputFormatterValidationRegistryArguments arguments)
{
    return SIMDCSVInputFormatIndexer::validateAndFormat(arguments.config);
}

std::ostream& operator<<(std::ostream& os, const SIMDCSVInputFormatIndexer&)
{
    return os << fmt::format("SIMDCSVInputFormatIndexer()");
}

}
