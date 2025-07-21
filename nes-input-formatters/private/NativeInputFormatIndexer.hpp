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

#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <fmt/format.h>
#include <Concepts.hpp>
#include <ErrorHandling.hpp>
#include <FieldIndexFunction.hpp>

namespace NES::InputFormatters
{

class NativeMetaData
{
public:
    NativeMetaData(ParserConfig, Schema) { /* noop */ }
    static std::string_view getTupleDelimitingBytes() { return ""; }
};

/// The NativeInputFormatter formats buffers that contain data which all other 'Operators' can operate on.
/// There is thus no need to parse the fields of the input data.
class NativeInputFormatIndexer
{
public:
    static constexpr bool IsFormattingRequired = false;
    static constexpr bool HasSpanningTuple = false;
    using IndexerMetaData = NativeMetaData;
    using FieldIndexFunctionType = NoopFormatter;

    void indexRawBuffer(NoopFormatter&, const RawTupleBuffer&, const NativeMetaData&) const
    {
        ///Noop
    }

    friend std::ostream& operator<<(std::ostream& os, const NativeInputFormatIndexer&)
    {
        return os << fmt::format("NativeInputFormatter()");
    }
};

static_assert(InputFormatIndexerType<NativeInputFormatIndexer>);

}
