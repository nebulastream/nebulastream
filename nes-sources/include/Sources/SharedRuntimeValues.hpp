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
#include <cstddef>
#include <initializer_list>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/RuntimeValue.hpp>

namespace NES
{
/// Immutable values plus ownership of their backing storage.
class SharedRuntimeValues
{
public:
    SharedRuntimeValues() = default;
    explicit SharedRuntimeValues(std::vector<RuntimeValue> values);
    SharedRuntimeValues(std::initializer_list<RuntimeValue> values);

    template <typename... T>
    [[nodiscard]] static SharedRuntimeValues make(T&&... values)
    {
        std::vector<RuntimeValue> owned;
        owned.reserve(sizeof...(T));
        (owned.emplace_back(std::forward<T>(values)), ...);
        return SharedRuntimeValues{std::move(owned)};
    }

    [[nodiscard]] size_t size() const;
    [[nodiscard]] bool empty() const;
    [[nodiscard]] RuntimeValueView operator[](size_t index) const&;
    RuntimeValueView operator[](size_t index) const&& = delete;

private:
    std::shared_ptr<const std::vector<RuntimeValue>> storage;
};
}
