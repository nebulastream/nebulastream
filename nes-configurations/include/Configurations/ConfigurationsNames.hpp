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

#include <array>
#include <cstdint>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

using namespace std::string_literals;

namespace NES::Configurations
{

/// TODO #417: remove enum
enum class OutputFormat : uint8_t
{
    CSV
};

class Names
{
public:
    enum class Option : uint8_t
    {
        CONFIG_PATH,
        TYPE_CONFIG,
        NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL,
        HAS_SPANNING_TUPLES
    };

    static constexpr bool contains(const Option key)
    {
        return not(std::views::filter(CONFIG_NAMES, [key](const auto& pair) { return pair.first == key; }).empty());
    }

    template <Option Key>
    static constexpr std::string_view get()
    {
        static_assert(contains(Key), "ConfigNames does not contain Key");
        return std::views::filter(CONFIG_NAMES, [](const auto& pair) { return pair.first == Key; }).begin()->second;
    }

    template <Option Key>
    static constexpr std::string getString()
    {
        static_assert(contains(Key), "ConfigNames does not contain Key");
        return std::string(std::views::filter(CONFIG_NAMES, [](const auto& pair) { return pair.first == Key; }).begin()->second);
    }

    static constexpr bool containsValue(const std::string_view value)
    {
        return not(std::views::filter(CONFIG_NAMES, [value](const auto& pair) { return pair.second == value; }).empty());
    }

private:
    static constexpr std::array<std::pair<Option, std::string_view>, 4> CONFIG_NAMES{
        std::make_pair(Option::CONFIG_PATH, "configPath"),
        std::make_pair(Option::TYPE_CONFIG, "type"),
        std::make_pair(Option::NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL, "numberOfBuffersInSourceLocalBufferPool"),
        std::make_pair(Option::HAS_SPANNING_TUPLES, "hasSpanningTuples")};
};

}
