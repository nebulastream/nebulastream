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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EMIT_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EMIT_HPP_
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>

namespace NES::Runtime::Execution::Operators {

class Emit : public ExecutableOperator {
  public:
    Emit(Runtime::MemoryLayouts::MemoryLayoutPtr resultMemoryLayout);
    void open(RuntimeExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void execute(RuntimeExecutionContext& ctx, Record& record) const override;
    void close(RuntimeExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    const uint64_t maxRecordsPerBuffer = 10;
    const Runtime::MemoryLayouts::MemoryLayoutPtr resultMemoryLayout;
};

}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EMIT_HPP_
