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
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <fmt/format.h>

#include <Configurations/Descriptor.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <NativeFormatFieldAccess.hpp>

namespace NES::InputFormatters
{

struct ConfigParametersNative
{
    static inline const std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap();
};

template <bool HasSpanningTuples>
class NativeInputFormatter final : public InputFormatter<NativeFormatFieldAccess<HasSpanningTuples>, /* IsNativeFormat */ true>
{
public:
    static constexpr std::string_view NAME = "Native";

    NativeInputFormatter() = default;
    ~NativeInputFormatter() override = default;

    NativeInputFormatter(const NativeInputFormatter&) = delete;
    NativeInputFormatter& operator=(const NativeInputFormatter&) = delete;
    NativeInputFormatter(NativeInputFormatter&&) = delete;
    NativeInputFormatter& operator=(NativeInputFormatter&&) = delete;

    void setupFieldAccessFunctionForBuffer(
        NativeFormatFieldAccess<HasSpanningTuples>& fieldAccessFunction,
        const RawTupleBuffer& rawBuffer,
        const TupleMetaData& tupleMetadata) const override
    {
        fieldAccessFunction.startSetup(rawBuffer, tupleMetadata);
    }

    static Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config)
    {
        return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersNative>(std::move(config), NAME);
    }

    [[nodiscard]] std::ostream& toString(std::ostream& os) const override { return os << fmt::format("NativeInputFormatter()"); }
};

}
