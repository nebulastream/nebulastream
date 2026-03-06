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

#include <Nautilus/Interface/Hash/BloomFilter.hpp>
#include <function.hpp>
#include <val.hpp>

namespace NES::Nautilus::Interface
{

/// Proxy function to call BloomFilter::mightContain from Nautilus JIT code.
/// Acts as a bridge between JIT-compiled code and the host-side BloomFilter.
inline bool mightContainProxy(const BloomFilter* filter, uint64_t value)
{
    if (filter == nullptr)
    {
        return true; // No filter = check everything
    }
    return filter->mightContain(value);
}

/**
 * @brief Nautilus-native wrapper for BloomFilter that can be used in JIT-compiled code.
 *
 * This is the Nautilus equivalent of BloomFilter, similar to how PagedVectorRef wraps PagedVector.
 * It allows BloomFilter operations to be called from within Nautilus-compiled code 
 */
class BloomFilterRef
{
public:
    /**
     * @brief Construct a BloomFilterRef from a Nautilus val<pointer>.
     * @param filter Nautilus val wrapping the host-side BloomFilter pointer
     */
    explicit BloomFilterRef(const nautilus::val<const BloomFilter*>& filter) : filterPtr(filter)
    {
    }

    /**
     * @brief Check if a value might be in the BloomFilter.
     *
     * This method can be called from Nautilus JIT-compiled code.
     * It uses the invoke mechanism to call back to the host-side BloomFilter.
     *
     * @param value The value to check (Nautilus val type for JIT compatibility)
     * @return nautilus::val<bool> - true if value might be present, false if definitely not present
     */
    [[nodiscard]] nautilus::val<bool> mightContain(nautilus::val<uint64_t> value) const
    {
        return nautilus::invoke(mightContainProxy, filterPtr, value);
    }

private:
    nautilus::val<const BloomFilter*> filterPtr;
};

} // namespace NES::Nautilus::Interface
