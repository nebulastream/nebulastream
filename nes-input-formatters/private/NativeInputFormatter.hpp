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

#include <InputFormatters/InputFormatter.hpp>
#include <fmt/format.h>
#include <NativeFormatFieldAccess.hpp>

namespace NES::InputFormatters
{

template <bool HasSpanningTuples>
class NativeInputFormatter final : public InputFormatter<NativeFormatFieldAccess<HasSpanningTuples>, /* IsNativeFormat */ true>
{
public:
    NativeInputFormatter() = default;
    ~NativeInputFormatter() override = default;

    NativeInputFormatter(const NativeInputFormatter&) = delete;
    NativeInputFormatter& operator=(const NativeInputFormatter&) = delete;
    NativeInputFormatter(NativeInputFormatter&&) = delete;
    NativeInputFormatter& operator=(NativeInputFormatter&&) = delete;

    void setupFieldAccessFunctionForBuffer(
        NativeFormatFieldAccess<HasSpanningTuples>& fieldAccessFunction,
        const BufferData& bufferData,
        const TupleMetaData& tupleMetadata) const override
    {
        fieldAccessFunction.startSetup(bufferData, tupleMetadata);
    }

    [[nodiscard]] std::ostream& toString(std::ostream& os) const override { return os << fmt::format("NativeInputFormatter()"); }
};

}
