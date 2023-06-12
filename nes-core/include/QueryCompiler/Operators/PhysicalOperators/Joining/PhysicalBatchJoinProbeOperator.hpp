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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_PHYSICALBATCHJOINPROBEOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_PHYSICALBATCHJOINPROBEOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES::QueryCompilation::PhysicalOperators::Experimental {
/**
 * @brief Physical operator for the batch join probe.
 * This operator queries the operator state and computes final join results.
 */
class PhysicalBatchJoinProbeOperator : public PhysicalBatchJoinOperator, public PhysicalUnaryOperator {
  public:
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& inputSchemaProbe,
                                      const SchemaPtr& outputSchema,
                                      const Join::Experimental::BatchJoinOperatorHandlerPtr& operatorHandler);
    static PhysicalOperatorPtr
    create(SchemaPtr inputSchemaProbe, SchemaPtr outputSchema, Join::Experimental::BatchJoinOperatorHandlerPtr operatorHandler);
    PhysicalBatchJoinProbeOperator(OperatorId id,
                                   SchemaPtr inputSchemaProbe,
                                   SchemaPtr outputSchema,
                                   Join::Experimental::BatchJoinOperatorHandlerPtr operatorHandler);
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;
};
}// namespace NES::QueryCompilation::PhysicalOperators::Experimental

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_PHYSICALBATCHJOINPROBEOPERATOR_HPP_
