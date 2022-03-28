#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EMIT_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EMIT_HPP_
#include <Interpreter/Operators/ExecuteOperator.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>

namespace NES::Interpreter {

class Emit : public ExecuteOperator {
  public:
    Emit(Runtime::MemoryLayouts::MemoryLayoutPtr resultMemoryLayout);
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
  private:
    const uint64_t maxRecordsPerBuffer = 10;
    const Runtime::MemoryLayouts::MemoryLayoutPtr resultMemoryLayout;
};

}// namespace NES::Interpreter
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EMIT_HPP_
