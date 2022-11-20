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
#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_OPERATORS_AGGREGATION_AGGREGATIONFUNCTION_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_OPERATORS_AGGREGATION_AGGREGATIONFUNCTION_HPP_
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/Operators/ExecutableOperator.hpp>

namespace NES::Nautilus {

class AggregationState {};

class GlobalSumState : public AggregationState {
  public:
    uint64_t sum;
};

class SumState : public AggregationState {
  public:
    SumState(Value<> value) : currentSum(value) {}
    Value<> currentSum;
};

class GlobalCountState : public AggregationState {
  public:
    uint64_t count;
};

class CountState : public AggregationState {
  public:
    CountState(Value<> value) : count(value) {}
    Value<> count;
};

class AggregationFunction {
  public:
    AggregationFunction(){};
    virtual ~AggregationFunction(){};
    virtual std::unique_ptr<AggregationState> createGlobalState() = 0;
    virtual std::unique_ptr<AggregationState> createState() = 0;
    virtual std::unique_ptr<AggregationState> loadState(Value<MemRef>& ref) = 0;
    virtual void storeState(Value<MemRef>& ref, std::unique_ptr<AggregationState>& state) = 0;
    virtual void liftCombine(std::unique_ptr<AggregationState>& ctx, Record& recordBuffer) = 0;
    virtual void combine(std::unique_ptr<AggregationState>& ctx1, std::unique_ptr<AggregationState>& ctx2) = 0;
    virtual Value<Any> lower(std::unique_ptr<AggregationState>& ctx) = 0;
    virtual uint64_t getStateSize() const = 0;
};

class SumFunction : public AggregationFunction {
  public:
    SumFunction(Runtime::Execution::Expressions::ExpressionPtr expression, Nautilus::IR::Types::StampPtr stamp);
    std::unique_ptr<AggregationState> createGlobalState() override;
    std::unique_ptr<AggregationState> createState() override;
    void liftCombine(std::unique_ptr<AggregationState>& ctx, Record& recordBuffer) override;
    void combine(std::unique_ptr<AggregationState>& ctx1, std::unique_ptr<AggregationState>& ctx2) override;
    Value<Any> lower(std::unique_ptr<AggregationState>& ctx) override;
    std::unique_ptr<AggregationState> loadState(Value<MemRef>& ref) override;
    void storeState(Value<MemRef>& ref, std::unique_ptr<AggregationState>& state) override;
    uint64_t getStateSize() const override;

  private:
    Runtime::Execution::Expressions::ExpressionPtr expression;
    Nautilus::IR::Types::StampPtr stamp;
};

class CountFunction : public AggregationFunction {
  public:
    CountFunction();
    std::unique_ptr<AggregationState> createGlobalState() override;
    std::unique_ptr<AggregationState> createState() override;
    void liftCombine(std::unique_ptr<AggregationState>& ctx, Record& recordBuffer) override;
    void combine(std::unique_ptr<AggregationState>& ctx1, std::unique_ptr<AggregationState>& ctx2) override;
    Value<Any> lower(std::unique_ptr<AggregationState>& ctx) override;
    std::unique_ptr<AggregationState> loadState(Value<MemRef>& ref) override;
    void storeState(Value<MemRef>& ref, std::unique_ptr<AggregationState>& state) override;
    uint64_t getStateSize() const override;
};

}// namespace NES::Nautilus
#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_OPERATORS_AGGREGATION_AGGREGATIONFUNCTION_HPP_
