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

#ifndef NES_PHYSICALSTREAMJOINBUILDOPERATOR_HPP
#define NES_PHYSICALSTREAMJOINBUILDOPERATOR_HPP

#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief This class represents the physical stream join build operator and gets translated to a StreamJoinBuild operator
 */
class PhysicalStreamJoinBuildOperator : public PhysicalStreamJoinOperator, public PhysicalUnaryOperator {
  public:
    /**
     * @brief creates a PhysicalStreamJoinSinkOperator with a provided operatorId
     * @param id
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param operatorHandler
     * @param buildSide
     * @param timeStampFieldName
     * @return PhysicalStreamJoinSinkOperator
     */
    static PhysicalOperatorPtr create(OperatorId id, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema,
                                      const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                      JoinBuildSideType buildSide, const std::string& timeStampFieldName);

    /**
     * @brief creates a PhysicalStreamJoinBuildOperator that retrieves a new operatorId by calling method
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param operatorHandler
     * @param buildSide
     * @param timeStampFieldName
     * @return PhysicalStreamJoinBuildOperator
     */
    static PhysicalOperatorPtr create(const SchemaPtr& inputSchema, const SchemaPtr& outputSchema,
                                      const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                      JoinBuildSideType buildSide, const std::string& timeStampFieldName);

    /**
     * @brief Constructor for PhysicalStreamJoinBuildOperator
     * @param id
     * @param inputSchema
     * @param outputSchema
     * @param operatorHandler
     * @param buildSide
     * @param timeStampFieldName
     */
    explicit PhysicalStreamJoinBuildOperator(OperatorId id,
                                             SchemaPtr inputSchema,
                                             SchemaPtr outputSchema,
                                             Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr operatorHandler,
                                             JoinBuildSideType buildSide,
                                             std::string  timeStampFieldName);

    ~PhysicalStreamJoinBuildOperator() noexcept override = default;

    [[nodiscard]] std::string toString() const override;

    OperatorNodePtr copy() override;

    JoinBuildSideType getBuildSide() const;

    const std::string& getTimeStampFieldName() const;

  private:
    std::string timeStampFieldName;
    JoinBuildSideType buildSide;

};

} // namespace NES::QueryCompilation::PhysicalOperators

#endif//NES_PHYSICALSTREAMJOINBUILDOPERATOR_HPP
