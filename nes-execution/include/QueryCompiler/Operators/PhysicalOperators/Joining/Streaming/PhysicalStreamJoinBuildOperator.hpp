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

#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALSTREAMJOINBUILDOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALSTREAMJOINBUILDOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
class PhysicalStreamJoinBuildOperator : public PhysicalStreamJoinOperator,
                                        public PhysicalUnaryOperator,
                                        public AbstractEmitOperator {
  public:
    /**
     * @brief creates a PhysicalStreamJoinBuildOperator with a provided operatorId
     * @param id
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param operatorHandler
     * @param buildSide
     * @param timeStampFieldName
     * @param joinFieldName
     * @return PhysicalStreamJoinSinkOperator
     */
    static PhysicalOperatorPtr create(OperatorId id,
                                      StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                      const JoinBuildSideType buildSide,
                                      const std::string& timeStampFieldName,
                                      const std::string& joinFieldName,
                                      QueryCompilation::StreamJoinStrategy joinStrategy,
                                      QueryCompilation::WindowingStrategy windowingStrategy);

    /**
     * @brief creates a PhysicalStreamJoinBuildOperator that retrieves a new operatorId by calling method
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param operatorHandler
     * @param buildSide
     * @param timeStampFieldName
     * @param joinFieldName
     * @return PhysicalStreamJoinBuildOperator
     */
    static PhysicalOperatorPtr create(StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                      const JoinBuildSideType buildSide,
                                      const std::string& timeStampFieldName,
                                      const std::string& joinFieldName,
                                      QueryCompilation::StreamJoinStrategy joinStrategy,
                                      QueryCompilation::WindowingStrategy windowingStrategy);

    /**
     * @brief Constructor for PhysicalStreamJoinBuildOperator
     * @param id
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     * @param inputSchema
     * @param outputSchema
     * @param operatorHandler
     * @param buildSide
     * @param timeStampFieldName
     * @param joinFieldName
     */
    explicit PhysicalStreamJoinBuildOperator(const OperatorId id,
                                             StatisticId statisticId,
                                             const SchemaPtr& inputSchema,
                                             const SchemaPtr& outputSchema,
                                             const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                             const JoinBuildSideType buildSide,
                                             const std::string& timeStampFieldName,
                                             const std::string& joinFieldName,
                                             QueryCompilation::StreamJoinStrategy joinStrategy,
                                             QueryCompilation::WindowingStrategy windowingStrategy);

    /**
     * @brief Deconstructor
     */
    ~PhysicalStreamJoinBuildOperator() noexcept override = default;

    /**
     * @brief Returns a string containing the name of this physical operator
     * @return String
     */
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Performs a deep copy of this physical operator
     * @return OperatorPtr
     */
    OperatorPtr copy() override;

    /**
     * @brief Getter for the build side, either left or right
     * @return JoinBuildSideType
     */
    JoinBuildSideType getBuildSide() const;

    /**
     * @brief Getter for the timestamp fieldname
     * @return String
     */
    const std::string& getTimeStampFieldName() const;

    /**
     * @brief Getter for the timestamp join field name
     * @return String
     */
    const std::string& getJoinFieldName() const;

  private:
    std::string timeStampFieldName;
    std::string joinFieldName;
    JoinBuildSideType buildSide;
};
}// namespace NES::QueryCompilation::PhysicalOperators
#endif // NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALSTREAMJOINBUILDOPERATOR_HPP_
