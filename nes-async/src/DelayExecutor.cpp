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
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

std::string requiredOption(const AsyncOperatorContext& context, const std::string& key)
{
    const auto it = context.config.find(key);
    if (it == context.config.end() || it->second.empty())
    {
        throw InvalidConfigParameter("DelayExecutor requires the option '{}'", key);
    }
    return it->second;
}

size_t fieldIndex(const AsyncRecordLayout& layout, const std::string& fieldName, const std::string& optionKey)
{
    const auto index = layout.indexOfColumn(fieldName);
    if (!index.has_value())
    {
        throw InvalidConfigParameter("DelayExecutor option '{}' names an unknown field '{}'", optionKey, fieldName);
    }
    return index.value();
}

}

DelayExecutor::DelayExecutor(AsyncOperatorContext operatorContext)
    : context(std::move(operatorContext))
    , delay(std::chrono::milliseconds{0})
    , inputFieldIndex(fieldIndex(*context.inputLayout, requiredOption(context, "input_field"), "input_field"))
    , outputFieldIndex(fieldIndex(*context.outputLayout, requiredOption(context, "output_field"), "output_field"))
{
    if (const auto it = context.config.find("delay_ms"); it != context.config.end())
    {
        const auto parsed = from_chars<uint64_t>(it->second);
        if (!parsed.has_value())
        {
            throw InvalidConfigParameter("DelayExecutor option 'delay_ms' must be a number, but was '{}'", it->second);
        }
        delay = std::chrono::milliseconds{parsed.value()};
    }
    if (const auto it = context.config.find("drop_prefix"); it != context.config.end() && !it->second.empty())
    {
        dropPrefix = it->second;
    }
}

std::vector<AsyncRecordResult> DelayExecutor::process(const std::span<const AsyncRecordView> batch)
{
    /// One wait per batch, not per record — the same shape a batched remote call has.
    if (delay.count() > 0)
    {
        std::this_thread::sleep_for(delay);
    }

    std::vector<AsyncRecordResult> results;
    results.reserve(batch.size());
    for (const auto& record : batch)
    {
        auto value = record.readAsText(inputFieldIndex);
        if (dropPrefix.has_value() && value.starts_with(*dropPrefix))
        {
            results.push_back(AsyncRecordResult{.fields = {}, .keep = false});
            continue;
        }
        std::ranges::transform(
            value, value.begin(), [](const unsigned char character) { return static_cast<char>(std::toupper(character)); });
        results.push_back(AsyncRecordResult{.fields = {AsyncFieldValue{.fieldIndex = outputFieldIndex, .value = std::move(value)}}});
    }
    return results;
}

}
