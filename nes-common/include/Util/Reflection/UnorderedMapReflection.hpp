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

#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <Util/Reflection/ReflectionCore.hpp>
#include <rfl/Generic.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Reflector/Unreflector for std::unordered_map<K, V, ...>
/// Only supports string or arithmetic key types
template <typename K, typename V, typename Hash, typename KeyEqual, typename Allocator>
requires(std::is_same_v<K, std::string> || std::is_arithmetic_v<K>)
struct Reflector<std::unordered_map<K, V, Hash, KeyEqual, Allocator>>
{
    Reflected operator()(const std::unordered_map<K, V, Hash, KeyEqual, Allocator>& data) const
    {
        rfl::Generic::Object obj;
        for (const auto& [key, value] : data)
        {
            std::string keyStr;
            if constexpr (std::is_same_v<K, std::string>)
            {
                keyStr = key;
            }
            else if constexpr (std::is_arithmetic_v<K>)
            {
                keyStr = std::to_string(key);
            }
            else
            {
                static_assert(
                    std::is_same_v<K, std::string> || std::is_arithmetic_v<K>, "Unsupported key type for std::unordered_map serialization");
            }
            obj[keyStr] = *reflect(value);
        }
        return Reflected{rfl::Generic::Object{std::move(obj)}};
    }
};

template <typename K, typename V, typename Hash, typename KeyEqual, typename Allocator>
requires(std::is_same_v<K, std::string> || std::is_arithmetic_v<K>)
struct Unreflector<std::unordered_map<K, V, Hash, KeyEqual, Allocator>>
{
    std::unordered_map<K, V, Hash, KeyEqual, Allocator> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        return std::visit(
            [&context](const auto& value) -> std::unordered_map<K, V, Hash, KeyEqual, Allocator>
            {
                using ValueType = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<ValueType, rfl::Generic::Object>)
                {
                    std::unordered_map<K, V, Hash, KeyEqual, Allocator> result;
                    for (const auto& [keyStr, valGeneric] : value)
                    {
                        /// Convert string key back to K
                        K key;
                        if constexpr (std::is_same_v<K, std::string>)
                        {
                            key = keyStr;
                        }
                        else if constexpr (std::is_arithmetic_v<K>)
                        {
                            if constexpr (std::is_integral_v<K>)
                            {
                                key = static_cast<K>(std::stoull(keyStr));
                            }
                            else
                            {
                                key = static_cast<K>(std::stod(keyStr));
                            }
                        }
                        else
                        {
                            static_assert(
                                std::is_same_v<K, std::string> || std::is_arithmetic_v<K>,
                                "Unsupported key type for std::unordered_map deserialization");
                        }
                        result[key] = context.unreflect<V>(Reflected{valGeneric});
                    }
                    return result;
                }
                throw CannotDeserialize("Expected object for std::unordered_map");
            },
            data->variant());
    }
};

}
