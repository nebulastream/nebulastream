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

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief The slice merging operator receives pre aggregated keyed slices and merges them together.
 * Finally, it appends them to the global slice store.
 */
class PhysicalKeyedSliceMergingOperator : public PhysicalUnaryOperator, public AbstractScanOperator {
  public:
    PhysicalKeyedSliceMergingOperator(OperatorId id,
                                      SchemaPtr inputSchema,
                                      SchemaPtr outputSchema,
                                      Windowing::Experimental::KeyedSliceMergingOperatorHandlerPtr operatorHandler);

    static std::shared_ptr<PhysicalKeyedSliceMergingOperator>
    create(SchemaPtr inputSchema,
           SchemaPtr outputSchema,
           Windowing::Experimental::KeyedSliceMergingOperatorHandlerPtr operatorHandler);

    Windowing::Experimental::KeyedSliceMergingOperatorHandlerPtr getWindowHandler();

    std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    Windowing::Experimental::KeyedSliceMergingOperatorHandlerPtr operatorHandler;
};

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_KEYEDTIMEWINDOW_PHYSICALKEYEDSLICEMERGINGOPERATOR_HPP_
