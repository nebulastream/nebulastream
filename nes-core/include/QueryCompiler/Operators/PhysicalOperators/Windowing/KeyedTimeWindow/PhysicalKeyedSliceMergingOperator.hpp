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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_KEYEDTIMEWINDOW_PHYSICALKEYEDSLICEMERGINGOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_KEYEDTIMEWINDOW_PHYSICALKEYEDSLICEMERGINGOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::Runtime::Execution::Operators {
class KeyedSliceMergingHandler;
}

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief The slice merging operator receives pre aggregated keyed slices and merges them together.
 * Finally, it appends them to the global slice store.
 */
class PhysicalKeyedSliceMergingOperator : public PhysicalUnaryOperator, public AbstractScanOperator {
  public:
    using WindowHandlerType = std::variant<std::shared_ptr<Runtime::Execution::Operators::KeyedSliceMergingHandler>>;
    PhysicalKeyedSliceMergingOperator(OperatorId id,
                                      SchemaPtr inputSchema,
                                      SchemaPtr outputSchema,
                                      WindowHandlerType operatorHandler,
                                      Windowing::LogicalWindowDefinitionPtr windowDefinition);

    /**
     * @brief Creates an instance of the physical operator
     * @param inputSchema
     * @param outputSchema
     * @param operatorHandler
     * @param windowDefinition
     * @return PhysicalKeyedSliceMergingOperatorPtr
     */
    static std::shared_ptr<PhysicalKeyedSliceMergingOperator>
    create(const SchemaPtr& inputSchema,
           const SchemaPtr& outputSchema,
           const WindowHandlerType& operatorHandler,
           const Windowing::LogicalWindowDefinitionPtr& windowDefinition);

    /**
     * @brief Returns the window handler of the slice merging operator
     * @return WindowHandlerType
     */
    WindowHandlerType getWindowHandler();

    /**
     * @brief Returns the window definition.
     * @return Windowing::LogicalWindowDefinitionPtr&
     */
    const Windowing::LogicalWindowDefinitionPtr& getWindowDefinition() const;
    std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    WindowHandlerType operatorHandler;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_KEYEDTIMEWINDOW_PHYSICALKEYEDSLICEMERGINGOPERATOR_HPP_
