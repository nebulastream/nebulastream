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
#include <string_view>

#include <DataTypes/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <UncompiledInputFormatters/UncompiledInputFormatterTaskPipeline.hpp>
#include <SIMDCSVKernel.hpp>
#include <UncompiledCSVInputFormatIndexer.hpp>
#include <UncompiledFieldBand.hpp>
#include <UncompiledInputFormatIndexer.hpp>

namespace NES
{

class SequentialUncompiledCSVInputFormatIndexer : public UncompiledInputFormatIndexer<SequentialUncompiledCSVInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SequentialUncompiledCSV";
    static constexpr bool IsFormattingRequired = true;
    static constexpr bool HasSpanningTuple = true;
    static constexpr bool IsSequential = true;
    using UncompiledIndexerMetaData = UncompiledCSVMetaData;
    using UncompiledFieldIndexFunctionType = UncompiledFieldBand;

    explicit SequentialUncompiledCSVInputFormatIndexer(InputFormatterDescriptor config, size_t numberOfFieldsInSchema);
    ~SequentialUncompiledCSVInputFormatIndexer() = default;

    void indexRawBuffer(UncompiledFieldBand& band, const UncompiledRawTupleBuffer& rawBuffer, const UncompiledCSVMetaData&) const;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SequentialUncompiledCSVInputFormatIndexer& obj);

private:
    char tupleDelimiter;
    char fieldDelimiter;
    size_t numberOfFieldsInSchema;
    /// allow_commas_in_strings: when false, the no-quote SIMD kernel is selected (skips the quote compare).
    bool allowCommasInStrings;
    /// Best block kernel for this CPU, resolved once at construction (runtime dispatch); do not call per buffer.
    SimdCsv::ComputeBlocksFn computeBlocks;
};

}
