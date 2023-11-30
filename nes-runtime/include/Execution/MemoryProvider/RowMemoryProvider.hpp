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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_MEMORYPROVIDER_ROWMEMORYPROVIDER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_MEMORYPROVIDER_ROWMEMORYPROVIDER_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

/**
 * @brief Implements MemoryProvider. Provides row-wise memory access.
 */
class RowMemoryProvider final : public MemoryProvider {
  public:
    /**
     * @brief Creates a row memory provider based on a valid row memory layout pointer.
     * @param Row memory layout pointer used to create the RowMemoryProvider.
     */
    RowMemoryProvider(Runtime::MemoryLayouts::RowLayoutPtr rowMemoryLayoutPtr);
    ~RowMemoryProvider() = default;

    MemoryLayouts::MemoryLayoutPtr getMemoryLayoutPtr() override;

    Nautilus::Record read(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                          Nautilus::Value<Nautilus::MemRef>& bufferAddress,
                          Nautilus::Value<Nautilus::UInt64>& recordIndex) const override;

    void write(Nautilus::Value<NES::Nautilus::UInt64>& recordIndex,
               Nautilus::Value<Nautilus::MemRef>& bufferAddress,
               NES::Nautilus::Record& rec) const override;

  private:
    Nautilus::Value<Nautilus::MemRef> calculateFieldAddress(Nautilus::Value<>& recordOffset, uint64_t fieldIndex) const;
    const Runtime::MemoryLayouts::RowLayoutPtr rowMemoryLayoutPtr;
};

}// namespace NES::Runtime::Execution::MemoryProvider
#endif  // NES_RUNTIME_INCLUDE_EXECUTION_MEMORYPROVIDER_ROWMEMORYPROVIDER_HPP_
