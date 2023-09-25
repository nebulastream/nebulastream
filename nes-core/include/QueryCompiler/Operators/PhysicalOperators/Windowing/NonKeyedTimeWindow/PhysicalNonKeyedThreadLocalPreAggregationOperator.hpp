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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_NONKEYEDTIMEWINDOW_PHYSICALNONKEYEDTHREADLOCALPREAGGREGATIONOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_NONKEYEDTIMEWINDOW_PHYSICALNONKEYEDTHREADLOCALPREAGGREGATIONOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::Runtime::Execution::Operators {
class NonKeyedSlicePreAggregationHandler;
class NonKeyedBucketPreAggregationHandler;
}// namespace NES::Runtime::Execution::Operators

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief The global thread local pre aggregation operator, receives an input source and creates on each processing thread an local pre aggregate.
 * If it receives a watermark the thread local slice is merges by the slice merging operator.
 */
class PhysicalNonKeyedThreadLocalPreAggregationOperator : public PhysicalUnaryOperator, public AbstractEmitOperator {
  public:
    using WindowHandlerType = std::variant<Windowing::Experimental::NonKeyedThreadLocalPreAggregationOperatorHandlerPtr,
                                           std::shared_ptr<Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler>,
                                           std::shared_ptr<Runtime::Execution::Operators::NonKeyedBucketPreAggregationHandler>>;
    PhysicalNonKeyedThreadLocalPreAggregationOperator(OperatorId id,
                                                      SchemaPtr inputSchema,
                                                      SchemaPtr outputSchema,
                                                      WindowHandlerType windowHandler,
                                                      Windowing::LogicalWindowDefinitionPtr windowDefinition);

    static std::shared_ptr<PhysicalOperator> create(SchemaPtr inputSchema,
                                                    SchemaPtr outputSchema,
                                                    WindowHandlerType windowHandler,
                                                    Windowing::LogicalWindowDefinitionPtr windowDefinition);

    std::string toString() const override;
    OperatorNodePtr copy() override;

    WindowHandlerType getWindowHandler() { return windowHandler; }

    const Windowing::LogicalWindowDefinitionPtr& getWindowDefinition() const;

  private:
    WindowHandlerType windowHandler;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_NONKEYEDTIMEWINDOW_PHYSICALNONKEYEDTHREADLOCALPREAGGREGATIONOPERATOR_HPP_
