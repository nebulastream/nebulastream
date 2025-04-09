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
#include <utility>

#include <InputFormatterRegistry.hpp>
#include <NativeFormatFieldAccess.hpp>
#include <NativeInputFormatter.hpp>

namespace NES::InputFormatters
{

InputFormatterRegistryReturnType InputFormatterGeneratedRegistrar::RegisterNativeInputFormatter(InputFormatterRegistryArguments arguments)
{
    if (arguments.inputFormatterConfig.hasSpanningTuples)
    {
        auto inputFormatter = std::make_unique<NativeInputFormatter<true>>();
        return arguments.createInputFormatterTaskPipeline<NativeInputFormatter<true>, NativeFormatFieldAccess<true>, true>(
            std::move(inputFormatter));
    }
    auto inputFormatter = std::make_unique<NativeInputFormatter<false>>();
    return arguments.createInputFormatterTaskPipeline<NativeInputFormatter<false>, NativeFormatFieldAccess<false>, false>(
        std::move(inputFormatter));
}

}
