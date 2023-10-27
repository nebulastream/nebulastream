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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>
#include <QueryCompiler/Phases/Translations/PhysicalOperatorProvider.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <vector>
namespace NES::QueryCompilation {

/**
 * @brief Stores a window operator and window definition, as well as in- and output schema
 */
struct WindowOperatorProperties {
    WindowOperatorNodePtr windowOperator;
    SchemaPtr windowInputSchema;
    SchemaPtr windowOutputSchema;
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
};

/**
 * @brief Stores all operator nodes for lowering the stream joins
 */
struct StreamJoinOperatorNodes {
    StreamJoinOperatorNodes(const LogicalOperatorNodePtr& operatorNode,
                            const OperatorNodePtr& leftInputOperator,
                            const OperatorNodePtr& rightInputOperator)
        : operatorNode(operatorNode), leftInputOperator(leftInputOperator), rightInputOperator(rightInputOperator) {}
    const LogicalOperatorNodePtr& operatorNode;
    const OperatorNodePtr& leftInputOperator;
    const OperatorNodePtr& rightInputOperator;
};

/**
 * @brief Stores all join configuration, e.g., window size, timestamp field name, join strategy, ...
 */
struct StreamJoinConfigs {
    StreamJoinConfigs(const std::string& joinFieldNameLeft,
                      const std::string& joinFieldNameRight,
                      const uint64_t windowSize,
                      const uint64_t windowSlide,
                      const std::string& timeStampFieldNameLeft,
                      const std::string& timeStampFieldNameRight,
                      const QueryCompilation::StreamJoinStrategy& joinStrategy)
        : joinFieldNameLeft(joinFieldNameLeft), joinFieldNameRight(joinFieldNameRight), windowSize(windowSize),
          windowSlide(windowSlide), timeStampFieldNameLeft(timeStampFieldNameLeft),
          timeStampFieldNameRight(timeStampFieldNameRight), joinStrategy(joinStrategy) {}

    const std::string& joinFieldNameLeft;
    const std::string& joinFieldNameRight;
    const uint64_t windowSize;
    const uint64_t windowSlide;
    const std::string& timeStampFieldNameLeft;
    const std::string& timeStampFieldNameRight;
    const QueryCompilation::StreamJoinStrategy& joinStrategy;
};

/**
 * @brief Provides a set of default lowerings for logical operators to corresponding physical operators.
 */
class DefaultPhysicalOperatorProvider : public PhysicalOperatorProvider {
  public:
    DefaultPhysicalOperatorProvider(QueryCompilerOptionsPtr options);
    static PhysicalOperatorProviderPtr create(const QueryCompilerOptionsPtr& options);
    void lower(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) override;
    virtual ~DefaultPhysicalOperatorProvider() noexcept = default;

  protected:
    /**
     * @brief Insets demultiplex operator before the current operator.
     * @param operatorNode
     */
    void insertDemultiplexOperatorsBefore(const LogicalOperatorNodePtr& operatorNode);
    /**
     * @brief Insert multiplex operator after the current operator.
     * @param operatorNode
     */
    void insertMultiplexOperatorsAfter(const LogicalOperatorNodePtr& operatorNode);
    /**
     * @brief Checks if the current operator is a demultiplexer, if it has multiple parents.
     * @param operatorNode
     * @return
     */
    bool isDemultiplex(const LogicalOperatorNodePtr& operatorNode);

    /**
     * @brief Lowers a binary operator
     * @param queryPlan current plan
     * @param operatorNode current operator
     */
    void lowerBinaryOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a unary operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerUnaryOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a union operator. However, A Union operator is not realized via executable code. It is realized by
    *        using a Multiplex operation that connects two sources with one sink. The two sources then form one stream 
    *        that continuously sends TupleBuffers to the sink. This means a query that only contains an Union operator 
    *        does not lead to code that is compiled and is entirely executed on the source/sink/TupleBuffer level.
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerUnionOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a project operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerProjectOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

#ifdef TFDEF
    /**
    * @brief Lowers an infer model operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerInferModelOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
#endif// TFDEF

    /**
    * @brief Lowers a map operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerMapOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a udf map operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerUDFMapOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a udf flat map operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerUDFFlatMapOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a window operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerWindowOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a thread local window operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerTimeBasedWindowOperator(const QueryPlanPtr&, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a watermark assignment operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerWatermarkAssignmentOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a join operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerJoinOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a join build operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    OperatorNodePtr getJoinBuildInputOperator(const JoinLogicalOperatorNodePtr& joinOperator,
                                              SchemaPtr schema,
                                              std::vector<OperatorNodePtr> children);

  private:
    /**
     * @brief replaces the window sink (and inserts a SliceStoreAppendOperator) depending on the time based window type for keyed windows
     * @param windowOperatorProperties
     * @param operatorNode
     */
    std::shared_ptr<Node> replaceOperatorNodeTimeBasedWindow(WindowOperatorProperties& windowOperatorProperties,
                                                             const LogicalOperatorNodePtr& operatorNode);

    /**
     * @brief Lowers a join operator for the nautilus query compiler
     * @param operatorNode
     */
    void lowerNautilusJoin(const LogicalOperatorNodePtr& operatorNode);

    /**
     * @brief Returns the left and right timestamp
     * @param joinOperator
     * @param windowType
     * @return {
     */
    [[nodiscard]] std::tuple<std::string, std::string>
    getTimestampLeftAndRight(const std::shared_ptr<JoinLogicalOperatorNode>& joinOperator,
                             const Windowing::TimeBasedWindowTypePtr& windowType) const;

    /**
     * @brief Lowers the stream hash join
     * @param streamJoinOperatorNodes
     * @param streamJoinConfig
     * @return StreamJoinOperatorHandlerPtr
     */
    Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr
    lowerStreamingHashJoin(const StreamJoinOperatorNodes& streamJoinOperatorNodes, const StreamJoinConfigs& streamJoinConfig);

    /**
     * @brief Lowers the stream nested loop join
     * @param streamJoinOperatorNodes
     * @param streamJoinConfig
     * @return StreamJoinOperatorHandlerPtr
     */
    Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr
    lowerStreamingNestedLoopJoin(const StreamJoinOperatorNodes& streamJoinOperatorNodes,
                                 const StreamJoinConfigs& streamJoinConfig);
};

}// namespace NES::QueryCompilation

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
