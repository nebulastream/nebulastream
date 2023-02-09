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
#ifndef NES_PHYSICALSTREAMJOINOPERATOR_HPP
#define NES_PHYSICALSTREAMJOINOPERATOR_HPP

#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief This class is the parent class of the PhysicalStreamJoinBuild and PhysicalStreamJoinSink operators.
 */
class PhysicalStreamJoinOperator : public AbstractEmitOperator {
  public:
    /**
     * @brief Getter for the StreamJoinOperatorHandler
     * @return StreamJoinOperatorHandler
     */
    [[nodiscard]] Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr getOperatorHandler() const;

    /**
     * @brief virtual deconstructor of PhysicalStreamJoinOperator
     */
    virtual ~PhysicalStreamJoinOperator() noexcept = default;

    /**
     * @brief Constructor for PhysicalStreamJoinOperator
     * @param operatorHandler
     * @param id
     */
    explicit PhysicalStreamJoinOperator(Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr operatorHandler,
                                        OperatorId id);

  protected:
    Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr operatorHandler;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif//NES_PHYSICALSTREAMJOINOPERATOR_HPP
