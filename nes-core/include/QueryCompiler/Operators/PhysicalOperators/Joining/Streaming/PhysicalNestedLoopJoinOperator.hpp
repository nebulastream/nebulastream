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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALNESTEDLOOPJOINOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALNESTEDLOOPJOINOPERATOR_HPP_

#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief This class is the parent class of the PhysicalNestedLoopJoinBuild and PhysicalNestedLoopJoinSink operators.
 */
class PhysicalNestedLoopJoinOperator : public AbstractEmitOperator {
  public:
    /**
     * @brief Getter for the NLJOperatorHandlerPtr
     * @return NLJOperatorHandlerPtr
     */
    [[nodiscard]] Runtime::Execution::Operators::NLJOperatorHandlerPtr getOperatorHandler() const;

    /**
     * @brief virtual deconstructor of PhysicalNestedLoopJoinOperator
     */
    virtual ~PhysicalNestedLoopJoinOperator() noexcept = default;

    /**
     * @brief Constructor for PhysicalNestedLoopJoinOperator
     * @param operatorHandler
     * @param id
     */
    explicit PhysicalNestedLoopJoinOperator(Runtime::Execution::Operators::NLJOperatorHandlerPtr operatorHandler,
                                      OperatorId id);

  protected:
    Runtime::Execution::Operators::NLJOperatorHandlerPtr operatorHandler;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALNESTEDLOOPJOINOPERATOR_HPP_
