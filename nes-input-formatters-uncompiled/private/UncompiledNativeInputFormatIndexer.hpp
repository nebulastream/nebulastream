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

#include <DataTypes/Schema.hpp>
#include <InputFormatters/UncompiledInputFormatterTaskPipeline.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <fmt/format.h>
#include <UncompiledFieldIndexFunction.hpp>
#include <UncompiledInputFormatIndexer.hpp>

namespace NES
{

class UncompiledNativeMetaData
{
public:
    UncompiledNativeMetaData(const ParserConfig&, const Schema&) { /* noop */ }

    static std::string_view getTupleDelimitingBytes() { return ""; }
};

/// The NativeInputFormatter formats buffers that contain data which all other 'Operators' can operate on.
/// There is thus no need to parse the fields of the input data.
class UncompiledNativeInputFormatIndexer : public UncompiledInputFormatIndexer<UncompiledNativeInputFormatIndexer>
{
public:
    static constexpr bool IsFormattingRequired = false;
    static constexpr bool HasSpanningTuple = false;
    static constexpr bool IsSequential = false;
    using UncompiledFieldIndexFunctionType = UncompiledNoopFieldIndexFunction;
    using UncompiledIndexerMetaData = UncompiledNativeMetaData;

    UncompiledNativeInputFormatIndexer() = default;

    void indexRawBuffer(UncompiledNoopFieldIndexFunction&, const UncompiledRawTupleBuffer&, const UncompiledNativeMetaData&) const
    {
        ///Noop
    }

    friend std::ostream& operator<<(std::ostream& os, const UncompiledNativeInputFormatIndexer&)
    {
        return os << fmt::format("NativeInputFormatter()");
    }
};

}
