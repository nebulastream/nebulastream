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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_MEMORYPROVIDER_MEMORYPROVIDER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_MEMORYPROVIDER_MEMORYPROVIDER_HPP_
#include "Runtime/RuntimeForwardRefs.hpp"
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

/**
 * @brief Abstract parent class for providing memory.
 */
class MemoryProvider {
  public:
    enum MemoryProviders{ COLUMN, ROW };
    
    // MemoryProvider() = default;
    virtual ~MemoryProvider();

    // virtual void setMemoryLayout(const Runtime::MemoryLayouts::MemoryLayoutPtr columnMemoryLayoutPtr);
    virtual MemoryLayouts::MemoryLayoutPtr getMemoryLayoutPtr() = 0;
    // virtual MemoryLayouts::MemoryLayoutPtr getMemoryLayoutPtr();

  private:
    // Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayoutPtr;
};

}// namespace NES::Runtime::Execution::MemoryProvider
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_MEMORYPROVIDER_MEMORYPROVIDER_HPP_
