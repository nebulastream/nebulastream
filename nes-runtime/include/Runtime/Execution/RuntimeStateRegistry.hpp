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
#include <cstdint>
#include <vector>
#include <ErrorHandling.hpp>

namespace NES
{
enum class RuntimeStateType : uint8_t
{
    SLICE_STORE_REF,
    INFERENCE_RUNTIME
};

class RuntimeStateRegistry final
{
public:
    uint64_t registerState(const RuntimeStateType type, void* const address)
    {
        PRECONDITION(address != nullptr, "Cannot register null runtime state");
        const auto slot = entries.size();
        entries.push_back(Entry{type, address});
        return slot;
    }

    [[nodiscard]] void* getState(const uint64_t slot, const RuntimeStateType type) const
    {
        if (slot >= entries.size() || entries[slot].type != type)
        {
            return nullptr;
        }
        return entries[slot].address;
    }

    void clear() { entries.clear(); }

private:
    struct Entry final
    {
        RuntimeStateType type;
        void* address;
    };

    std::vector<Entry> entries;
};
}
