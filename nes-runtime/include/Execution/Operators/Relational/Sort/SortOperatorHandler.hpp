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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORTOPERATORHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORTOPERATORHANDLER_HPP_

#include <Nautilus/Interface/Stack/Stack.hpp>
#include <Nautilus/Interface/Stack/StackRef.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

#include <vector>

namespace NES::Runtime::Execution::Operators {

class SortOperatorHandler : public Runtime::Execution::OperatorHandler,
                         public NES::detail::virtual_enable_shared_from_this<SortOperatorHandler, false> {
  public:
    SortOperatorHandler(uint64_t entrySize) : entrySize(entrySize){};

    void setup(Runtime::Execution::PipelineExecutionContext&) {
        auto allocator = std::make_unique<NesDefaultMemoryAllocator>();
        stack = std::make_unique<Nautilus::Interface::Stack>(std::move(allocator), entrySize);
        tempStack = std::make_unique<Nautilus::Interface::Stack>(std::move(allocator), entrySize);
    }

    void start(Runtime::Execution::PipelineExecutionContextPtr,
               Runtime::StateManagerPtr,
               uint32_t) override {};

    void stop(Runtime::QueryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr) override {};

    uint64_t getCount() { return stack->getNumberOfEntries(); }

    Nautilus::Interface::Stack *getState() { return stack.get(); }
    Nautilus::Interface::StackRef getStateRef() {
        return Nautilus::Interface::StackRef(Nautilus::Value<Nautilus::MemRef>((int8_t*) &tempStack), entrySize);
    }

    Nautilus::Interface::Stack *getTempState() { return tempStack.get(); }

    uint64_t getEntrySize() const { return entrySize; }

  private:
    std::unique_ptr<Nautilus::Interface::Stack> stack;
    std::unique_ptr<Nautilus::Interface::Stack> tempStack;
    uint64_t entrySize;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORTOPERATORHANDLER_HPP_
