/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALSLICESINKOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALSLICESINKOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical slice sink operator.
 * A slice sink, passes pre-aggregated slices to the downstream operator.
 * A slice is a non overlapping fraction of one or multiple final windows.
 */
class PhysicalSliceSinkOperator : public PhysicalWindowOperator {
  public:
    PhysicalSliceSinkOperator(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition);
    static PhysicalOperatorPtr create(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition);
    static PhysicalOperatorPtr create(Windowing::LogicalWindowDefinitionPtr windowDefinition);
    const std::string toString() const override;
    OperatorNodePtr copy() override;

};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALSLICESINKOPERATOR_HPP_
