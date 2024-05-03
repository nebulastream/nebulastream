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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <QueryCompiler/Phases/Translations/PhysicalOperatorProvider.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <vector>
namespace NES::QueryCompilation {

/**
 * @brief Stores a window operator and window definition, as well as in- and output schema
 */
struct WindowOperatorProperties {
    WindowOperatorProperties(WindowOperatorPtr windowOperator,
                             SchemaPtr windowInputSchema,
                             SchemaPtr windowOutputSchema,
                             Windowing::LogicalWindowDescriptorPtr windowDefinition)
        : windowOperator(std::move(windowOperator)), windowInputSchema(std::move(windowInputSchema)),
          windowOutputSchema(std::move(windowOutputSchema)), windowDefinition(std::move(windowDefinition)){};

    WindowOperatorPtr windowOperator;
    SchemaPtr windowInputSchema;
    SchemaPtr windowOutputSchema;
    Windowing::LogicalWindowDescriptorPtr windowDefinition;
};

/**
 * @brief Stores all operator nodes for lowering the stream joins
 */
struct StreamJoinOperators {
    StreamJoinOperators(const LogicalOperatorPtr& operatorNode,
                        const OperatorPtr& leftInputOperator,
                        const OperatorPtr& rightInputOperator)
        : operatorNode(operatorNode), leftInputOperator(leftInputOperator), rightInputOperator(rightInputOperator) {}
    const LogicalOperatorPtr& operatorNode;
    const OperatorPtr& leftInputOperator;
    const OperatorPtr& rightInputOperator;
};

/**
 * @brief Stores all join configuration, e.g., window size, timestamp field name, join strategy, ...
 */
struct StreamJoinConfigs {
    StreamJoinConfigs(const std::string& joinFieldNameLeft,
                      const std::string& joinFieldNameRight,
                      const uint64_t windowSize,
                      const uint64_t windowSlide,
                      const TimestampField& timeStampFieldLeft,
                      const TimestampField& timeStampFieldRight,
                      const QueryCompilation::StreamJoinStrategy& joinStrategy)
        : joinFieldNameLeft(joinFieldNameLeft), joinFieldNameRight(joinFieldNameRight), windowSize(windowSize),
          windowSlide(windowSlide), timeStampFieldLeft(timeStampFieldLeft), timeStampFieldRight(timeStampFieldRight),
          joinStrategy(joinStrategy) {}

    const std::string& joinFieldNameLeft;
    const std::string& joinFieldNameRight;
    const uint64_t windowSize;
    const uint64_t windowSlide;
    const TimestampField& timeStampFieldLeft;
    const TimestampField& timeStampFieldRight;
    const QueryCompilation::StreamJoinStrategy& joinStrategy;
};

/**
 * @brief Provides a set of default lowerings for logical operators to corresponding physical operators.
 */
class DefaultPhysicalOperatorProvider : public PhysicalOperatorProvider {
  public:
    DefaultPhysicalOperatorProvider(QueryCompilerOptionsPtr options);
    static PhysicalOperatorProviderPtr create(const QueryCompilerOptionsPtr& options);
    void lower(DecomposedQueryPlanPtr decomposedQueryPlan, LogicalOperatorPtr operatorNode) override;
    virtual ~DefaultPhysicalOperatorProvider() noexcept = default;

  protected:
    /**
     * @brief Insets demultiplex operator before the current operator.
     * @param operatorNode
     */
    void insertDemultiplexOperatorsBefore(const LogicalOperatorPtr& operatorNode);
    /**
     * @brief Insert multiplex operator after the current operator.
     * @param operatorNode
     */
    void insertMultiplexOperatorsAfter(const LogicalOperatorPtr& operatorNode);
    /**
     * @brief Checks if the current operator is a demultiplexer, if it has multiple parents.
     * @param operatorNode
     * @return
     */
    bool isDemultiplex(const LogicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a binary operator
     * @param operatorNode current operator
     */
    void lowerBinaryOperator(const LogicalOperatorPtr& operatorNode);

    /**
    * @brief Lowers a unary operator
    * @param decomposedQueryPlan current plan
    * @param operatorNode current operator
    */
    void lowerUnaryOperator(const DecomposedQueryPlanPtr& decomposedQueryPlan, const LogicalOperatorPtr& operatorNode);

    /**
    * @brief Lowers a union operator. However, A Union operator is not realized via executable code. It is realized by
    *        using a Multiplex operation that connects two sources with one sink. The two sources then form one stream 
    *        that continuously sends TupleBuffers to the sink. This means a query that only contains an Union operator 
    *        does not lead to code that is compiled and is entirely executed on the source/sink/TupleBuffer level.
    * @param operatorNode current operator
    */
    void lowerUnionOperator(const LogicalOperatorPtr& operatorNode);

    /**
    * @brief Lowers a project operator
    * @param operatorNode current operator
    */
    void lowerProjectOperator(const LogicalOperatorPtr& operatorNode);

    /**
    * @brief Lowers an infer model operator
    * @param operatorNode current operator
    */
    void lowerInferModelOperator(LogicalOperatorPtr operatorNode);

    /**
    * @brief Lowers a map operator
    * @param operatorNode current operator
    */
    void lowerMapOperator(const LogicalOperatorPtr& operatorNode);

    /**
    * @brief Lowers a udf map operator
    * @param operatorNode current operator
    */
    void lowerUDFMapOperator(const LogicalOperatorPtr& operatorNode);

    /**
    * @brief Lowers a udf flat map operator
    * @param operatorNode current operator
    */
    void lowerUDFFlatMapOperator(const LogicalOperatorPtr& operatorNode);

    /**
    * @brief Lowers a window operator
    * @param operatorNode current operator
    */
    void lowerWindowOperator(const LogicalOperatorPtr& operatorNode);

    /**
    * @brief Lowers a thread local window operator
    * @param operatorNode current operator
    */
    void lowerTimeBasedWindowOperator(const LogicalOperatorPtr& operatorNode);

    /**
    * @brief Lowers a watermark assignment operator
    * @param operatorNode current operator
    */
    void lowerWatermarkAssignmentOperator(const LogicalOperatorPtr& operatorNode);

    /**
    * @brief Lowers a join operator
    * @param operatorNode current operator
    */
    void lowerJoinOperator(const LogicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a statistic build operator
     * @param logicalStatisticWindowOperator
     */
    void lowerStatisticBuildOperator(Statistic::LogicalStatisticWindowOperator& logicalStatisticWindowOperator);

    /**
     * @brief Get a join build input generator
     * @param joinOperator join operator
     * @param schema the operator schema
     * @param children the upstream operators
     */
    OperatorPtr
    getJoinBuildInputOperator(const LogicalJoinOperatorPtr& joinOperator, SchemaPtr schema, std::vector<OperatorPtr> children);

  private:
    /**
     * @brief replaces the window sink (and inserts a SliceStoreAppendOperator) depending on the time based window type for keyed windows
     * @param windowOperatorProperties
     * @param operatorNode
     */
    std::shared_ptr<Node> replaceOperatorTimeBasedWindow(WindowOperatorProperties& windowOperatorProperties,
                                                         const LogicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a join operator for the nautilus query compiler
     * @param operatorNode
     */
    void lowerNautilusJoin(const LogicalOperatorPtr& operatorNode);

    /**
     * @brief Returns the left and right timestamp
     * @param joinOperator
     * @param windowType
     * @return {
     */
    [[nodiscard]] std::tuple<TimestampField, TimestampField>
    getTimestampLeftAndRight(const std::shared_ptr<LogicalJoinOperator>& joinOperator,
                             const Windowing::TimeBasedWindowTypePtr& windowType) const;

    /**
     * @brief Lowers the stream hash join
     * @param streamJoinOperators
     * @param streamJoinConfig
     * @return StreamJoinOperatorHandlerPtr
     */
    Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr
    lowerStreamingHashJoin(const StreamJoinOperators& streamJoinOperators, const StreamJoinConfigs& streamJoinConfig);

    /**
     * @brief Lowers the stream nested loop join
     * @param streamJoinOperators
     * @param streamJoinConfig
     * @return StreamJoinOperatorHandlerPtr
     */
    Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr
    lowerStreamingNestedLoopJoin(const StreamJoinOperators& streamJoinOperators, const StreamJoinConfigs& streamJoinConfig);
};

}// namespace NES::QueryCompilation

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
