#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFACTORY_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFACTORY_HPP_

#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>

namespace NES {

class LogicalOperatorFactory {
  public:
    template<typename Operator, typename... Arguments>
    static std::shared_ptr<Operator> create(Arguments&&... args) {
        return std::shared_ptr<Operator>(std::forward<Arguments>(args)...);
    }

    /**
     * @brief Create a new logical filter operator.
     * @param predicate the filter predicate is represented as an expression node, which has to return true.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createFilterOperator(const ExpressionNodePtr predicate);

    /**
     * @brief Create a new sink operator with a specific sink descriptor.
     * @param sinkDescriptor the SinkDescriptor.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createSinkOperator(const SinkDescriptorPtr sinkDescriptor);

    /**
     * @brief Create a new map operator with a field assignment expression as a map expression.
     * @param mapExpression the FieldAssignmentExpressionNode.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createMapOperator(const FieldAssignmentExpressionNodePtr mapExpression);

    /**
     * @brief Create a new source operator with source descriptor.
     * @param sourceDescriptor the SourceDescriptorPtr.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createSourceOperator(const SourceDescriptorPtr sourceDescriptor);

    /**
     * @brief Create a new window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createWindowOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition);

    /**
     * @brief Create a specialized central window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createCentralWindowSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition);

    /**
     * @brief Create a specialized slice creation window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createSliceCreationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition);

    /**
     * @brief Create a specialized slice merging window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createSliceMergingSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition);

    /**
     * @brief Create a specialized window computation window operator with window definition.
     * @param windowDefinition the LogicalWindowDefinitionPtr.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createWindowComputationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition);

    /**
    * @brief Create a specialized merge operator with window definition.
    * @param windowDefinition the LogicalWindowDefinitionPtr.
    * @return LogicalOperatorNodePtr
    */
    static LogicalOperatorNodePtr createMergeOperator();

    /**
     * @brief Create a broadcast operator.
     * @return LogicalOperatorNodePtr
     */
    static LogicalOperatorNodePtr createBroadcastOperator();
};

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFACTORY_HPP_
