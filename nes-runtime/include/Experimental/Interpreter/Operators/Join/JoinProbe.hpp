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
#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_OPERATORS_JOIN_JOINPROBE_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_OPERATORS_JOIN_JOINPROBE_HPP_
#include <Execution/Expressions/Expression.hpp>
#include <Experimental/Interpreter/Operators/ExecutableOperator.hpp>
#include <Experimental/Interpreter/Operators/Join/JoinBuild.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <vector>

namespace NES::Nautilus {

class JoinProbe : public ExecutableOperator {
  public:
    JoinProbe(std::shared_ptr<NES::Experimental::Hashmap> hashmap,
              std::vector<Record::RecordFieldIdentifier> resultFields,
              std::vector<Runtime::Execution::Expressions::ExpressionPtr> keyExpressions,
              std::vector<Runtime::Execution::Expressions::ExpressionPtr> valueExpressions,
              std::vector<Nautilus::IR::Types::StampPtr> hashmapKeyStamps,
              std::vector<Nautilus::IR::Types::StampPtr> hashmapValueStamps);
    void setup(RuntimeExecutionContext& executionCtx) const override;
    void execute(RuntimeExecutionContext& ctx, Record& record) const override;

  private:
    std::shared_ptr<NES::Experimental::Hashmap> hashMap;
    const std::vector<Record::RecordFieldIdentifier> resultFields;
    const std::vector<Runtime::Execution::Expressions::ExpressionPtr> keyExpressions;
    const std::vector<Runtime::Execution::Expressions::ExpressionPtr> valueExpressions;
    const std::vector<Nautilus::IR::Types::StampPtr> keyTypes;
    const std::vector<Nautilus::IR::Types::StampPtr> valueTypes;
    mutable uint32_t tag;
};

}// namespace NES::Nautilus
#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_OPERATORS_JOIN_JOINPROBE_HPP_
