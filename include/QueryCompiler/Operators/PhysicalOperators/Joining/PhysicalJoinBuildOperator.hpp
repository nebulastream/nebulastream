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
#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_PHYSICAL_OPERATORS_JOINING_PHYSICAL_JOIN_BUILD_OPERATOR_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_PHYSICAL_OPERATORS_JOINING_PHYSICAL_JOIN_BUILD_OPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
/**
 * @brief Physical operator for the join build.
 * This operator receives input records and adds them to its operator state.
 */
class PhysicalJoinBuildOperator : public PhysicalJoinOperator, public PhysicalUnaryOperator, public AbstractEmitOperator {
  public:
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Join::JoinOperatorHandlerPtr& operatorHandler,
                                      JoinBuildSide buildSide);
    static PhysicalOperatorPtr
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, Join::JoinOperatorHandlerPtr operatorHandler, JoinBuildSide buildSide);
    PhysicalJoinBuildOperator(OperatorId id,
                              SchemaPtr inputSchema,
                              SchemaPtr outputSchema,
                              Join::JoinOperatorHandlerPtr operatorHandler,
                              JoinBuildSide buildSide);

    ~PhysicalJoinBuildOperator() noexcept override = default;

    std::string toString() const override;
    OperatorNodePtr copy() override;

    JoinBuildSide getBuildSide();

  private:
    JoinBuildSide joinBuildSide;
};
}// namespace NES::QueryCompilation::PhysicalOperators

#endif  // NES_INCLUDE_QUERY_COMPILER_OPERATORS_PHYSICAL_OPERATORS_JOINING_PHYSICAL_JOIN_BUILD_OPERATOR_HPP_
