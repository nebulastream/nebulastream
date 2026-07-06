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

#include <UncompiledCSVInputFormatIndexer.hpp>

#include <cstddef>
#include <ostream>
#include <string>
#include <utility>

#include <Sources/SourceDescriptor.hpp>
#include <UncompiledInputFormatters/UncompiledInputFormatterTaskPipeline.hpp>
#include <fmt/format.h>
#include <InputFormatterValidationRegistry.hpp>
#include <SIMDCSVKernel.hpp>
#include <SIMDCSVScan.hpp>
#include <UncompiledFieldBand.hpp>
#include <UncompiledInputFormatIndexerRegistry.hpp>
#include <UncompiledInputFormatterTask.hpp>

namespace NES
{

UncompiledCSVInputFormatIndexer::UncompiledCSVInputFormatIndexer(InputFormatterDescriptor config, const size_t numberOfFieldsInSchema)
    : tupleDelimiter(config.getFromConfig(ConfigParametersUncompiledCSVInputFormatIndexer::TUPLE_DELIMITER).front())
    , fieldDelimiter(config.getFromConfig(ConfigParametersUncompiledCSVInputFormatIndexer::FIELD_DELIMITER).front())
    , numberOfFieldsInSchema(numberOfFieldsInSchema)
    , allowCommasInStrings(config.getFromConfig(ConfigParametersUncompiledCSVInputFormatIndexer::ALLOW_COMMAS_IN_STRINGS))
    /// Member-init order: allowCommasInStrings is declared before computeBlocks, so it is set here.
    , computeBlocks(SimdCsv::selectComputeBlocks(allowCommasInStrings))
{
}

void UncompiledCSVInputFormatIndexer::indexRawBuffer(
    UncompiledFieldBand& band, const UncompiledRawTupleBuffer& rawBuffer, const UncompiledCSVMetaData&) const
{
    /// Reuse the compiled parallel fast path: the branchless SIMD flat band (== SIMDCSVInputFormatIndexer).
    SimdCsv::indexBandInto(
        band, rawBuffer.getBufferView(), fieldDelimiter, tupleDelimiter, allowCommasInStrings, numberOfFieldsInSchema, computeBlocks);
}

DescriptorConfig::Config UncompiledCSVInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersUncompiledCSVInputFormatIndexer>(std::move(config), NAME);
}

UncompiledInputFormatIndexerRegistryReturnType RegisterUncompiledCSVUncompiledInputFormatIndexer(
    UncompiledInputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createUncompiledInputFormatterTaskPipeline(
        UncompiledCSVInputFormatIndexer(arguments.inputFormatIndexerConfig, arguments.getNumberOfFieldsInSchema()),
        UncompiledQuotationType::NONE);
}

InputFormatterValidationRegistryReturnType InputFormatterValidationGeneratedRegistrar::RegisterUncompiledCSVInputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return UncompiledCSVInputFormatIndexer::validateAndFormat(arguments.config);
}

std::ostream& operator<<(std::ostream& os, const UncompiledCSVInputFormatIndexer& obj)
{
    return os << fmt::format(
               "UncompiledCSVInputFormatIndexer(tupleDelimiter: {}, fieldDelimiter: {})", obj.tupleDelimiter, obj.fieldDelimiter);
}
}
