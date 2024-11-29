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

#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_INTERVALJOIN_PHYSICALINTERVALJOINBUILDOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_INTERVALJOIN_PHYSICALINTERVALJOINBUILDOPERATOR_HPP_

#include <Execution/Operators/Streaming/Join/IntervalJoin/IJOperatorHandler.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Util/Common.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
class PhysicalIntervalJoinBuildOperator : public PhysicalUnaryOperator, public AbstractEmitOperator {
  public:
    /**
     * @brief Creates a PhysicalIntervalJoinBuildOperator with the provided operatorId
     * @return PhysicalStreamJoinSinkOperator
     */
    static PhysicalOperatorPtr create(OperatorId id,
                                      StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& otherBuildSideInputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Runtime::Execution::Operators::IJOperatorHandlerPtr& operatorHandler,
                                      const JoinBuildSideType buildSide,
                                      TimestampField timestampFieldThisSide,
                                      TimestampField timestampFieldOtherSide);

    /**
     * @brief Creates a PhysicalIntervalJoinBuildOperator that retrieves a new operatorId by calling method
     * @return PhysicalStreamJoinBuildOperator
     */
    static PhysicalOperatorPtr create(StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& otherBuildSideInputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Runtime::Execution::Operators::IJOperatorHandlerPtr& operatorHandler,
                                      const JoinBuildSideType buildSide,
                                      TimestampField timestampFieldThisSide,
                                      TimestampField timestampFieldOtherSide);

    /**
     * @brief Constructor for PhysicalStreamJoinBuildOperator
     */
    explicit PhysicalIntervalJoinBuildOperator(const OperatorId id,
                                               StatisticId statisticId,
                                               const SchemaPtr& inputSchema,
                                               const SchemaPtr& otherBuildSideInputSchema,
                                               const SchemaPtr& outputSchema,
                                               const Runtime::Execution::Operators::IJOperatorHandlerPtr& operatorHandler,
                                               const JoinBuildSideType buildSide,
                                               TimestampField timestampFieldThisSide,
                                               TimestampField timestampFieldOtherSide);

    ~PhysicalIntervalJoinBuildOperator() noexcept override = default;

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
     * @brief Getter for the timestamp field of the build side
     * @return String
     */
    const TimestampField& getTimeStampFieldThisSide() const;

    /**
     * @brief Getter for the timestamp field of the other build side
     * @return String
     */
    const TimestampField& getTimeStampFieldOtherSide() const;

    /**
     * @brief Getter for the operator handler
     * @return IJOperatorHandler
     */
    const Runtime::Execution::Operators::IJOperatorHandlerPtr& getIJOperatorHandler() const;

    /**
     * @brief Getter the schema of the other join side
     * @return String
     */
    const SchemaPtr& getOtherSideInputSchema() const;

  private:
    TimestampField timeStampFieldThisSide;
    TimestampField timeStampFieldOtherSide;
    JoinBuildSideType buildSide;
    SchemaPtr otherSideInputSchema = Schema::create();
    Runtime::Execution::Operators::IJOperatorHandlerPtr operatorHandler;
};
}// namespace NES::QueryCompilation::PhysicalOperators
#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_INTERVALJOIN_PHYSICALINTERVALJOINBUILDOPERATOR_HPP_
