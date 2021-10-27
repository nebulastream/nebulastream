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
#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_PHYSICAL_OPERATORS_PHYSICAL_EMIT_OPERATOR_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_PHYSICAL_OPERATORS_PHYSICAL_EMIT_OPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical Emit operator.
 */
class PhysicalEmitOperator : public PhysicalUnaryOperator, public AbstractEmitOperator {
  public:
    /**
     * @brief Constructor for the physical emit operator
     * @param id operator id
     * @param inputSchema input schema for the emit operator
     */
    PhysicalEmitOperator(OperatorId id, const SchemaPtr& inputSchema);

    /**
     * @brief Creates a physical emit operator
     * @param id operator id
     * @param inputSchema
     * @return PhysicalOperatorPtr
     */
    static PhysicalOperatorPtr create(OperatorId id, const SchemaPtr& inputSchema);

    /**
     * @brief Creates a physical emit operator
     * @param inputSchema
     * @return PhysicalOperatorPtr
     */
    static PhysicalOperatorPtr create(SchemaPtr inputSchema);

    std::string toString() const override;

    OperatorNodePtr copy() override;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERY_COMPILER_OPERATORS_PHYSICAL_OPERATORS_PHYSICAL_EMIT_OPERATOR_HPP_
