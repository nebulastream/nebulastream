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

#include <SequentialCSVInputFormatIndexer.hpp>

#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <fmt/format.h>
#include <CSVInputFormatIndexer.hpp>
#include <FieldBand.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatter.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <InputFormatterValidationRegistry.hpp>
#include <RawTupleBuffer.hpp>
#include <SIMDCSVKernel.hpp>
#include <SIMDCSVScan.hpp>

namespace NES
{

SequentialCSVInputFormatIndexer::SequentialCSVInputFormatIndexer(const InputFormatterDescriptor& config)
    : allowCommasInStrings(config.getFromConfig(ConfigParametersCSVInputFormatIndexer::ALLOW_COMMAS_IN_STRINGS))
    , computeBlocks(SimdCsv::selectComputeBlocks())
{
}

void SequentialCSVInputFormatIndexer::indexRawBuffer(FieldBand& band, const RawTupleBuffer& rawBuffer, const CSVMetaData& metaData) const
{
    /// Branchless SIMD flat-band scan with the synthetic leading anchor (aligned buffer -> tuple 0 at byte 0).
    SimdCsv::indexBandSequentialInto(
        band,
        rawBuffer.getBufferView(),
        metaData.getFieldDelimiter(),
        metaData.getTupleDelimiter(),
        allowCommasInStrings,
        metaData.getNumberOfFields(),
        computeBlocks);
}

DescriptorConfig::Config SequentialCSVInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCSVInputFormatIndexer>(std::move(config), NAME);
}

InputFormatIndexerRegistryReturnType
RegisterSequentialCSVInputFormatIndexer(InputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createInputFormatterWithIndexer(SequentialCSVInputFormatIndexer{arguments.getInputFormatterConfig()});
}

InputFormatterValidationRegistryReturnType InputFormatterValidationGeneratedRegistrar::RegisterSequentialCSVInputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return SequentialCSVInputFormatIndexer::validateAndFormat(arguments.config);
}

std::ostream& operator<<(std::ostream& os, const SequentialCSVInputFormatIndexer&)
{
    return os << fmt::format("SequentialCSVInputFormatIndexer()");
}
}
