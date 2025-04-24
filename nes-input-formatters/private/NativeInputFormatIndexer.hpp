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

#include <InputFormatters/InputFormatIndexer.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES::InputFormatters
{

/// The NativeInputFormatter formats buffers that contain data which all other 'Operators' can operate on.
/// There is thus no need to parse the fields of the input data.
template <bool HasSpanningTuple>
class NativeInputFormatIndexer final : public InputFormatIndexer<struct NoopFormatter, /* IsFormattingRequired */ false>
{
public:
    NativeInputFormatIndexer() = default;
    ~NativeInputFormatIndexer() override = default;

    void indexRawBuffer(NoopFormatter&, const RawTupleBuffer&, const TupleMetaData&) const override
    {
        INVARIANT(not HasSpanningTuple, "The Native input formatter currently does not support spanning tuples.");
    }

    [[nodiscard]] std::ostream& toString(std::ostream& os) const override { return os << fmt::format("NativeInputFormatter()"); }
};

}
