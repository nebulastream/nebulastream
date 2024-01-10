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
#ifndef NES_EXECUTION_INCLUDE_EXECUTION_MEMORYPROVIDER_COLUMNMEMORYPROVIDER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_MEMORYPROVIDER_COLUMNMEMORYPROVIDER_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

/**
 * @brief Implements MemoryProvider. Provides columnar memory access.
 */
class ColumnMemoryProvider final : public MemoryProvider {
  public:
    /**
     * @brief Creates a column memory provider based on a valid column memory layout pointer.
     * @param Column memory layout pointer used to create the ColumnMemoryProvider.
     */
    ColumnMemoryProvider(Runtime::MemoryLayouts::ColumnLayoutPtr columnMemoryLayoutPtr);
    ~ColumnMemoryProvider() = default;

    MemoryLayouts::MemoryLayoutPtr getMemoryLayoutPtr() override;

    Nautilus::Record read(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                          Nautilus::Value<Nautilus::MemRef>& bufferAddress,
                          Nautilus::Value<Nautilus::UInt64>& recordIndex) const override;

    void write(Nautilus::Value<NES::Nautilus::UInt64>& recordIndex,
               Nautilus::Value<Nautilus::MemRef>& bufferAddress,
               NES::Nautilus::Record& rec) const override;

  private:
    Nautilus::Value<Nautilus::MemRef> calculateFieldAddress(Nautilus::Value<Nautilus::MemRef>& bufferAddress,
                                                            Nautilus::Value<Nautilus::UInt64>& recordIndex,
                                                            uint64_t fieldIndex) const;
    Runtime::MemoryLayouts::ColumnLayoutPtr columnMemoryLayoutPtr;
};

}// namespace NES::Runtime::Execution::MemoryProvider
#endif // NES_EXECUTION_INCLUDE_EXECUTION_MEMORYPROVIDER_COLUMNMEMORYPROVIDER_HPP_
