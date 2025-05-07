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

#include <memory>
#include <optional>
#include <utility>

#include <Util/Strings.hpp>
#include <InputFormatterRegistry.hpp>
#include <InputFormatterValidationRegistry.hpp>
#include <NativeFormatFieldAccess.hpp>
#include <NativeInputFormatter.hpp>

namespace NES::InputFormatters
{

InputFormatterValidationRegistryReturnType InputFormatterValidationGeneratedRegistrar::RegisterNativeInputFormatterValidation(
    InputFormatterValidationRegistryArguments inputFormatterConfig)
{
    if (const auto hasSpanningTupleString = inputFormatterConfig.config.find("hasSpanningTuple");
        hasSpanningTupleString != inputFormatterConfig.config.end())
    {
        if (Util::from_chars<bool>(hasSpanningTupleString->second))
        {
            return NativeInputFormatter<true>::validateAndFormat(std::move(inputFormatterConfig.config));
        }
    }
    return NativeInputFormatter<false>::validateAndFormat(std::move(inputFormatterConfig.config));
}

InputFormatterRegistryReturnType InputFormatterGeneratedRegistrar::RegisterNativeInputFormatter(InputFormatterRegistryArguments arguments)
{
    if (arguments.inputFormatterConfig.getHasSpanningTuples())
    {
        auto inputFormatter = std::make_unique<NativeInputFormatter<true>>();
        return arguments.createInputFormatterTaskPipeline<NativeInputFormatter<true>, NativeFormatFieldAccess<true>, true>(
            std::move(inputFormatter), std::nullopt);
    }
    auto inputFormatter = std::make_unique<NativeInputFormatter<false>>();
    return arguments.createInputFormatterTaskPipeline<NativeInputFormatter<false>, NativeFormatFieldAccess<false>, false>(
        std::move(inputFormatter), std::nullopt);
}

}
