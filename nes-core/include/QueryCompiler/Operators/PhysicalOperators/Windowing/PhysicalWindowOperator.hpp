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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALWINDOWOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALWINDOWOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical operator for all window sinks.
 * A window sink computes the final window result using the window slice store.
 */
class PhysicalWindowOperator : public PhysicalUnaryOperator {
  public:
    PhysicalWindowOperator(OperatorId id,
                           SchemaPtr inputSchema,
                           SchemaPtr outputSchema,
                           Windowing::WindowOperatorHandlerPtr operatorHandler);

    /**
    * @brief Gets the window handler of the window operator.
    * @return WindowOperatorHandlerPtr
    */
    Windowing::WindowOperatorHandlerPtr getOperatorHandler() const;

  protected:
    Windowing::WindowOperatorHandlerPtr operatorHandler;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES
#endif  // NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALWINDOWOPERATOR_HPP_
