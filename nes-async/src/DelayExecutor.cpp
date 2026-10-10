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

#include <Async/DelayExecutor.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncOperatorOptions.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
constexpr std::string_view Name = "DelayExecutor";
}

DelayExecutor::DelayExecutor(AsyncOperatorContext operatorContext)
    : context(std::move(operatorContext))
    , delay(std::chrono::milliseconds{static_cast<std::chrono::milliseconds::rep>(numericOption(context, "delay_ms", 0, Name))})
    , failOn(findOption(context, "fail_on"))
    , inputFieldIndex(requiredFieldIndex(*context.inputLayout, requiredOption(context, "input_field", Name), "input_field", Name))
    , outputFieldIndex(requiredFieldIndex(*context.outputLayout, requiredOption(context, "output_field", Name), "output_field", Name))
{
}

std::vector<AsyncRecordResult> DelayExecutor::process(const std::span<const AsyncRecordView> batch)
{
    /// One wait per batch, not per record — the same shape a batched remote call has.
    if (delay.count() > 0)
    {
        std::this_thread::sleep_for(delay);
    }

    if (failOn.has_value())
    {
        for (const auto& record : batch)
        {
            if (record.readAsText(inputFieldIndex) == *failOn)
            {
                throw UnknownOperation("DelayExecutor was configured to fail on '{}'", *failOn);
            }
        }
    }

    std::vector<AsyncRecordResult> results;
    results.reserve(batch.size());
    for (const auto& record : batch)
    {
        auto value = record.readAsText(inputFieldIndex);
        std::ranges::transform(
            value, value.begin(), [](const unsigned char character) { return static_cast<char>(std::toupper(character)); });
        results.push_back(AsyncRecordResult{.fields = {AsyncFieldValue{.fieldIndex = outputFieldIndex, .value = std::move(value)}}});
    }
    return results;
}

}
