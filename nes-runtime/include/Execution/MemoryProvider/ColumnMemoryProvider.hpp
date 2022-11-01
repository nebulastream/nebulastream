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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_MEMOMRYPROVIDER_COLUMNMEMORYPROVIDER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_MEMOMRYPROVIDER_COLUMNMEMORYPROVIDER_HPP_

#include "Runtime/RuntimeForwardRefs.hpp"
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

/**
 * @brief Implements MemoryProvider. Provides columnar memory access.
 */
class ColumnMemoryProvider : public MemoryProvider {
  public:
    /**
     * @brief Creates a column memory provider based on a valid column memory layout pointer.
     * @param Column memory layout pointer used to create the ColumnMemoryProvider.
     */
    ColumnMemoryProvider(Runtime::MemoryLayouts::MemoryLayoutPtr columnMemoryLayoutPtr);
    ~ColumnMemoryProvider() = default;

    /**
     * @brief Return a base-class memory layout pointer.
     * 
     * @return MemoryLayouts::MemoryLayoutPtr: Pointer to base-class memory layout.
     */
    MemoryLayouts::MemoryLayoutPtr getMemoryLayoutPtr() override;

    /**
     * @brief Return the memory layout as a column layout pointer. Returns nullptr if layout is not column layout.
     * 
     * @return MemoryLayouts::ColumnLayoutPtr: Is nullptr, if layout is not a column layout.
     */
    // MemoryLayouts::ColumnLayoutPtr getColumnMemoryLayoutPtr();

  private:
    const Runtime::MemoryLayouts::MemoryLayoutPtr columnMemoryLayoutPtr;
};

}// namespace NES::Runtime::Execution::MemoryProvider
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_MEMORYPROVIDER_COLUMNMEMORYPROVIDER_HPP_
