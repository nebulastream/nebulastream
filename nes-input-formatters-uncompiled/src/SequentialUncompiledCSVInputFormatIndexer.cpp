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

#include <SequentialUncompiledCSVInputFormatIndexer.hpp>

#include <cstddef>
#include <ostream>
#include <string>
#include <string_view>
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

SequentialUncompiledCSVInputFormatIndexer::SequentialUncompiledCSVInputFormatIndexer(
    InputFormatterDescriptor config, const size_t numberOfFieldsInSchema)
    : tupleDelimiter(config.getFromConfig(ConfigParametersUncompiledCSVInputFormatIndexer::TUPLE_DELIMITER).front())
    , fieldDelimiter(config.getFromConfig(ConfigParametersUncompiledCSVInputFormatIndexer::FIELD_DELIMITER).front())
    , numberOfFieldsInSchema(numberOfFieldsInSchema)
    , allowCommasInStrings(config.getFromConfig(ConfigParametersUncompiledCSVInputFormatIndexer::ALLOW_COMMAS_IN_STRINGS))
    /// Member-init order: allowCommasInStrings is declared before computeBlocks, so it is set here.
    , computeBlocks(SimdCsv::selectComputeBlocks(allowCommasInStrings))
{
}

void SequentialUncompiledCSVInputFormatIndexer::indexRawBuffer(
    UncompiledFieldBand& band, const UncompiledRawTupleBuffer& rawBuffer, const UncompiledCSVMetaData&) const
{
    /// Same PARALLEL flat band as the compiled SequentialCSVInputFormatIndexer (b0 at the first newline):
    /// the leading row [0 -> firstNewline] is NOT in the band -- the sequential task glue completes it from
    /// its single-threaded carry accumulator (leading spanning tuple), the bulk between the first and last
    /// newline is parsed in place, and the trailing partial is carried to the next buffer. Byte 0 of the
    /// glue's re-indexed `<delim><record><delim>` spanning buffer is a delimiter, which this handles too
    /// (b0 == 0, one complete tuple).
    SimdCsv::indexBandInto(
        band, rawBuffer.getBufferView(), fieldDelimiter, tupleDelimiter, allowCommasInStrings, numberOfFieldsInSchema, computeBlocks);
}

DescriptorConfig::Config SequentialUncompiledCSVInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersUncompiledCSVInputFormatIndexer>(std::move(config), NAME);
}

UncompiledInputFormatIndexerRegistryReturnType RegisterSequentialUncompiledCSVUncompiledInputFormatIndexer(
    UncompiledInputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createUncompiledInputFormatterTaskPipeline(
        SequentialUncompiledCSVInputFormatIndexer(arguments.inputFormatIndexerConfig, arguments.getNumberOfFieldsInSchema()),
        UncompiledQuotationType::NONE);
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterSequentialUncompiledCSVInputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return SequentialUncompiledCSVInputFormatIndexer::validateAndFormat(arguments.config);
}

std::ostream& operator<<(std::ostream& os, const SequentialUncompiledCSVInputFormatIndexer& obj)
{
    return os << fmt::format(
               "SequentialUncompiledCSVInputFormatIndexer(tupleDelimiter: {}, fieldDelimiter: {})", obj.tupleDelimiter, obj.fieldDelimiter);
}
}
