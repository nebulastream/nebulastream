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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFACTORY_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFACTORY_HPP_

#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/StatisticMetric.hpp>
#include <Operators/LogicalOperators/StatisticCollection/WindowStatisticDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <Operators/Operator.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>

namespace NES {

class LogicalOperatorFactory {
  public:
    template<typename Operator, typename... Arguments>
    static std::shared_ptr<Operator> create(Arguments&&... args) {
        return std::shared_ptr<Operator>(std::forward<Arguments>(args)...);
    }

    static LogicalUnaryOperatorPtr createReorderTuplesOperator(OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new logical filter operator.
     * @param predicate: the filter predicate is represented as an expression node, which has to return true.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr createFilterOperator(ExpressionNodePtr const& predicate, OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new source rename operator.
     * @param new source name
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr createRenameSourceOperator(std::string const& newSourceName,
                                                              OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new logical limit operator.
     * @param limit number of tuples to output
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr createLimitOperator(const uint64_t limit, OperatorId id = getNextOperatorId());

    /**
    * @brief Create a new logical projection operator.
    * @param expression list
    * @param id: the id of the operator if not defined then next free operator id is used.
    * @return LogicalOperatorPtr
    */
    static LogicalUnaryOperatorPtr createProjectionOperator(const std::vector<ExpressionNodePtr>& expressions,
                                                            OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new sink operator with a specific sink descriptor.
     * @param sinkDescriptor the SinkDescriptor.
     * @param workerId: the id of the worker where the sink is placed
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorPtr
     */
    static LogicalUnaryOperatorPtr createSinkOperator(SinkDescriptorPtr const& sinkDescriptor,
                                                      WorkerId workerId = INVALID_WORKER_NODE_ID,
                                                      OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new map operator with a field assignment expression as a map expression.
     * @param mapExpression the FieldAssignmentExpressionNode.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr createMapOperator(FieldAssignmentExpressionNodePtr const& mapExpression,
                                                     OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new infer model operator.
     * @param model: The path to the model of the operator.
     * @param inputFields: The intput fields of the model.
     * @param outputFields: The output fields of the model.
     * @param id: The id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr createInferModelOperator(std::string model,
                                                            std::vector<ExpressionNodePtr> inputFields,
                                                            std::vector<ExpressionNodePtr> outputFields,
                                                            OperatorId id = getNextOperatorId());

    /**
     * @brief Creates a synopsis build operator
     * @param window: Window properties
     * @param statisticDescriptor: Descriptor on how to build the statistic
     * @param metricHash: The hash of the metric, this operator is collecting, e.g., `cardinality` over field `f1`
     * @param sendingPolicy: Policy so when and how to send the data
     * @param triggerCondition: Policy when and how to call the callback method
     * @param id: The id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorNodePtr
     */
    static LogicalUnaryOperatorPtr
    createStatisticBuildOperator(const Windowing::WindowTypePtr& window,
                                 const Statistic::WindowStatisticDescriptorPtr& statisticDescriptor,
                                 const Statistic::StatisticMetricHash metricHash,
                                 const Statistic::SendingPolicyPtr sendingPolicy,
                                 const Statistic::TriggerConditionPtr triggerCondition,
                                 OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new source operator with source descriptor.
     * @param sourceDescriptor the SourceDescriptorPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr createSourceOperator(SourceDescriptorPtr const& sourceDescriptor,
                                                        OperatorId id = getNextOperatorId(),
                                                        OriginId originId = INVALID_ORIGIN_ID);

    /**
    * @brief Create a specialized watermark assigner operator.
    * @param watermarkStrategy strategy to be used to assign the watermark
    * @param id: the id of the operator if not defined then next free operator id is used.
    * @return LogicalOperatorPtr
    */
    static LogicalUnaryOperatorPtr
    createWatermarkAssignerOperator(Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor,
                                    OperatorId id = getNextOperatorId());
    /**
     * @brief Create a new window operator with window definition.
     * @param windowDefinition the LogicalWindowDescriptorPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr createWindowOperator(Windowing::LogicalWindowDescriptorPtr const& windowDefinition,
                                                        OperatorId id = getNextOperatorId());

    /**
     * @brief Create a specialized union operator.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return BinaryOperator
     */
    static LogicalBinaryOperatorPtr createUnionOperator(OperatorId id = getNextOperatorId());

    /**
    * @brief Create a specialized join operator.
    * @param id: the id of the operator if not defined then next free operator id is used.
    * @return BinaryOperator
    */
    static LogicalBinaryOperatorPtr createJoinOperator(const Join::LogicalJoinDescriptorPtr& joinDefinition,
                                                       OperatorId id = getNextOperatorId());

    // todo put in experimental namespace
    /**
    * @brief Create a specialized batch join operator.
    * @param id: the id of the operator if not defined then next free operator id is used.
    * @return BinaryOperator
    */
    static LogicalBinaryOperatorPtr
    createBatchJoinOperator(const Join::Experimental::LogicalBatchJoinDescriptorPtr& batchJoinDefinition,
                            OperatorId id = getNextOperatorId());

    /**
    * @brief Create a specialized interval join operator.
    * @param id: the id of the operator if not defined then next free operator id is used.
    * @return BinaryOperator
    */
    static LogicalBinaryOperatorPtr
    createIntervalJoinOperator(const Join::LogicalIntervalJoinDescriptorPtr& intervalJoinDefinition,
                               OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new MapJavaUDFLogicalOperator.
     * @param javaUdfDescriptor The descriptor of the Java UDF represented by this logical operator node.
     * @param id The operator ID.
     * @return A logical operator node which encapsulates the Java UDF.
     */
    static LogicalUnaryOperatorPtr createMapUDFLogicalOperator(const Catalogs::UDF::UDFDescriptorPtr udfDescriptor,
                                                               OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new FlatMapJavaUDFLogicalOperator.
     * @param javaUdfDescriptor The descriptor of the Java UDF represented by this logical operator node.
     * @param id The operator ID.
     * @return A logical operator node which encapsulates the Java UDF.
     */
    static LogicalUnaryOperatorPtr createFlatMapUDFLogicalOperator(const Catalogs::UDF::UDFDescriptorPtr udfDescriptor,
                                                                   OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new OpenCL logical operator
     * @param javaUdfDescriptor : the java UDF descriptor
     * @param id : the id of the operator
     * @return a logical operator of type OpenCL logical operator
     */
    static LogicalUnaryOperatorPtr createOpenCLLogicalOperator(const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor,
                                                               OperatorId id = getNextOperatorId());
};

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFACTORY_HPP_
