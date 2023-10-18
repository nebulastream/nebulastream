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

#ifndef NES_NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALSTREAMJOINOPERATOR_HPP_
#define NES_NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALSTREAMJOINOPERATOR_HPP_

#include <Execution/Operators/Streaming/Join/StreamJoinOperator.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief Abstract class that represents a physical stream join build or probe operator
 */
class PhysicalStreamJoinOperator {
  public:
    /**
     * @brief Constructor for a PhysicalStreamJoinOperator
     * @param joinOperatorHandler
     * @param joinStrategy
     * @param windowingStrategy
     */
    PhysicalStreamJoinOperator(const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& joinOperatorHandler,
                               QueryCompilation::StreamJoinStrategy joinStrategy,
                               QueryCompilation::WindowingStrategy windowingStrategy);

    /**
     * @brief Getter for the join strategy
     * @return StreamJoinStrategy
     */
    StreamJoinStrategy getJoinStrategy() const;

    /**
     * @brief Getter for the window strategy
     * @return WindowingStrategy
     */
    WindowingStrategy getWindowingStrategy() const;

    const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& getJoinOperatorHandler() const;

  protected:
    Runtime::Execution::Operators::StreamJoinOperator streamJoinOperator;
    Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr joinOperatorHandler;
};
}// namespace NES::QueryCompilation::PhysicalOperators
#endif//NES_NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALSTREAMJOINOPERATOR_HPP_