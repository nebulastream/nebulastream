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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_MEMOMRYPROVIDER_ROWMEMORYPROVIDER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_MEMOMRYPROVIDER_ROWMEMORYPROVIDER_HPP_

#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

/**
 * @brief Implements MemoryProvider. Provides row-wise memory access.
 */
class RowMemoryProvider : public MemoryProvider {
  public:
    /**
     * @brief Creates a row memory provider based on a valid row memory layout pointer.
     * @param Row memory layout pointer used to create the RowMemoryProvider.
     */
    RowMemoryProvider(Runtime::MemoryLayouts::MemoryLayoutPtr rowMemoryLayoutPtr);
    ~RowMemoryProvider() = default;

    /**
     * @brief Return a base-class memory layout pointer.
     * 
     * @return MemoryLayouts::MemoryLayoutPtr: Pointer to base-class memory layout.
     */
    MemoryLayouts::MemoryLayoutPtr getMemoryLayoutPtr() override;

    /**
     * @brief Return the memory layout as a row layout pointer. Returns nullptr if layout is not row layout.
     * 
     * @return MemoryLayouts::RowLayoutPtr: Is nullptr, if layout is not a row layout.
     */
    // MemoryLayouts::RowLayoutPtr getRowMemoryLayoutPtr();

  private:
    const Runtime::MemoryLayouts::MemoryLayoutPtr rowMemoryLayoutPtr;
};

}// namespace NES::Runtime::Execution::MemoryProvider
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_MEMORYPROVIDER_ROWMEMORYPROVIDER_HPP_
