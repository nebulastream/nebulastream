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

#include <Async/AsyncOperatorOptions.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

std::optional<std::string> findOption(const AsyncOperatorContext& context, const std::string_view key)
{
    const auto it = context.config.find(std::string{key});
    if (it == context.config.end() || it->second.empty())
    {
        return std::nullopt;
    }
    return it->second;
}

std::string requiredOption(const AsyncOperatorContext& context, const std::string_view key, const std::string_view executorName)
{
    if (auto value = findOption(context, key))
    {
        return std::move(value).value();
    }
    throw InvalidConfigParameter("{} requires the option '{}'", executorName, key);
}

uint64_t
numericOption(const AsyncOperatorContext& context, const std::string_view key, const uint64_t fallback, const std::string_view executorName)
{
    const auto value = findOption(context, key);
    if (!value.has_value())
    {
        return fallback;
    }
    const auto parsed = from_chars<uint64_t>(value.value());
    if (!parsed.has_value())
    {
        throw InvalidConfigParameter("{} option '{}' must be a number, but was '{}'", executorName, key, value.value());
    }
    return parsed.value();
}

size_t requiredFieldIndex(
    const AsyncRecordLayout& layout,
    const std::string_view fieldName,
    const std::string_view optionKey,
    const std::string_view executorName)
{
    const auto index = layout.indexOfColumn(fieldName);
    if (!index.has_value())
    {
        throw InvalidConfigParameter("{} option '{}' names an unknown field '{}'", executorName, optionKey, fieldName);
    }
    return index.value();
}

}
