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

namespace NES::StoreManager
{

/// Policy that determines when to trigger a flush from one level to the next.
struct FlushPolicy
{
    enum class Type : uint8_t
    {
        SIZE_THRESHOLD
    };
    Type type = Type::SIZE_THRESHOLD;
    size_t sizeThreshold = 64UZ * 1024UZ * 1024UZ; /// 64 MB /// NOLINT(readability-magic-numbers)

    /// Check whether the current size exceeds the threshold and a flush should be triggered.
    [[nodiscard]] bool shouldFlush(uint64_t currentSize) const
    {
        switch (type)
        {
            case Type::SIZE_THRESHOLD:
                return currentSize >= sizeThreshold;
        }
        return false;
    }
};

}
