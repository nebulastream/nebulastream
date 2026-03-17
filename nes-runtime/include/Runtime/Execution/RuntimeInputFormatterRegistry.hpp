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

#include <cstdint>
#include <unordered_map>

namespace NES
{
class RuntimeInputFormatterRegistry final
{
public:
    void registerHandle(uint64_t runtimeInputFormatterKey, std::uintptr_t runtimeInputFormatterHandle)
    {
        runtimeInputFormatterHandles.insert_or_assign(runtimeInputFormatterKey, runtimeInputFormatterHandle);
    }

    [[nodiscard]] std::uintptr_t getHandle(uint64_t runtimeInputFormatterKey) const
    {
        const auto handleIterator = runtimeInputFormatterHandles.find(runtimeInputFormatterKey);
        return handleIterator == runtimeInputFormatterHandles.end() ? 0 : handleIterator->second;
    }

private:
    std::unordered_map<uint64_t, std::uintptr_t> runtimeInputFormatterHandles;
};
}
