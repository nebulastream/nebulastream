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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_GLOBALTIMEWINDOW_PHYSICALGLOBALSLICEMERGINGOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_GLOBALTIMEWINDOW_PHYSICALGLOBALSLICEMERGINGOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <variant>

namespace NES::Runtime::Execution::Operators {
class GlobalSliceMergingHandler;
}

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief The slice merging operator receives pre aggregated keyed slices and merges them together.
 * Finally, it appends them to the global slice store.
 */
class PhysicalGlobalSliceMergingOperator : public PhysicalUnaryOperator, public AbstractScanOperator {


  public:
    using WindowHandlerType =
        std::variant<Windowing::Experimental::GlobalSliceMergingOperatorHandlerPtr,
                     std::shared_ptr<Runtime::Execution::Operators::GlobalSliceMergingHandler>>;
    PhysicalGlobalSliceMergingOperator(OperatorId id,
                                       SchemaPtr inputSchema,
                                       SchemaPtr outputSchema,
                                       WindowHandlerType operatorHandler,
                                       Windowing::LogicalWindowDefinitionPtr windowDefinition);

    static std::shared_ptr<PhysicalGlobalSliceMergingOperator>
    create(SchemaPtr inputSchema,
           SchemaPtr outputSchema,
           WindowHandlerType operatorHandler,
           Windowing::LogicalWindowDefinitionPtr windowDefinition);

    WindowHandlerType getWindowHandler();

    std::string toString() const override;
    OperatorNodePtr copy() override;
    const Windowing::LogicalWindowDefinitionPtr& getWindowDefinition() const;

  private:
    WindowHandlerType operatorHandler;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_GLOBALTIMEWINDOW_PHYSICALGLOBALSLICEMERGINGOPERATOR_HPP_
