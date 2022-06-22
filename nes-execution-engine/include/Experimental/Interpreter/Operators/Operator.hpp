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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OPERATOR_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OPERATOR_HPP_
#include <memory>

namespace NES::ExecutionEngine::Experimental::Interpreter {
class RecordBuffer;
class ExecutableOperator;
class ExecutionContext;
using ExecuteOperatorPtr = std::shared_ptr<ExecutableOperator>;
class TraceContext;
class Operator {
  public:

    virtual void setup(ExecutionContext& executionCtx) const;
    /**
     * @brief Open is called for each record buffer and is used to initializes execution local state.
     * @param recordBuffer
     */
    virtual void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    virtual void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void setChild(ExecuteOperatorPtr child);
    virtual ~Operator();

  private:
    bool hasChildren() const;

  protected:
    mutable ExecuteOperatorPtr child;
};

}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OPERATOR_HPP_
