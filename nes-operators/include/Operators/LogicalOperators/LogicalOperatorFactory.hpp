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

#pragma once

#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/Operator.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

class LogicalOperatorFactory
{
public:
    template <typename Operator, typename... Arguments>
    static std::shared_ptr<Operator> create(Arguments&&... args)
    {
        return std::shared_ptr<Operator>(std::forward<Arguments>(args)...);
    }

    /**
     * @brief Create a new logical filter operator.
     * @param predicate: the filter predicate is represented as an function node, which has to return true.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr createFilterOperator(NodeFunctionPtr const& predicate, OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new source rename operator.
     * @param new source name
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr createRenameSourceOperator(std::string const& newSourceName, OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new logical limit operator.
     * @param limit number of tuples to output
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr createLimitOperator(const uint64_t limit, OperatorId id = getNextOperatorId());

    /**
    * @brief Create a new logical projection operator.
    * @param function list
    * @param id: the id of the operator if not defined then next free operator id is used.
    * @return LogicalOperatorPtr
    */
    static LogicalUnaryOperatorPtr
    createProjectionOperator(const std::vector<NodeFunctionPtr>& functions, OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new sink operator with a specific sink descriptor.
     * @param sinkDescriptor the SinkDescriptor.
     * @param workerId: the id of the worker where the sink is placed
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return LogicalOperatorPtr
     */
    static LogicalUnaryOperatorPtr createSinkOperator(
        SinkDescriptorPtr const& sinkDescriptor, WorkerId workerId = INVALID_WORKER_NODE_ID, OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new map operator with a field assignment function as a map function.
     * @param mapFunction the NodeFunctionFieldAssignment.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr
    createMapOperator(NodeFunctionFieldAssignmentPtr const& mapFunction, OperatorId id = getNextOperatorId());

    /**
     * @brief Create a new infer model operator.
     * @param model: The path to the model of the operator.
     * @param inputFields: The intput fields of the model.
     * @param outputFields: The output fields of the model.
     * @param id: The id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr createInferModelOperator(
        std::string model,
        std::vector<NodeFunctionPtr> inputFields,
        std::vector<NodeFunctionPtr> outputFields,
        OperatorId id = getNextOperatorId());

    static LogicalUnaryOperatorPtr createSourceOperator(std::string logicalSourceName, OperatorId id = getNextOperatorId());

    static LogicalUnaryOperatorPtr
    createSourceOperator(std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor, OperatorId id = getNextOperatorId());

    static LogicalUnaryOperatorPtr createSourceOperator(
        std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor,
        OperatorId id = getNextOperatorId(),
        OriginId originId = INVALID_ORIGIN_ID);

    /**
    * @brief Create a specialized watermark assigner operator.
    * @param watermarkStrategy strategy to be used to assign the watermark
    * @param id: the id of the operator if not defined then next free operator id is used.
    * @return LogicalOperatorPtr
    */
    static LogicalUnaryOperatorPtr createWatermarkAssignerOperator(
        Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor, OperatorId id = getNextOperatorId());
    /**
     * @brief Create a new window operator with window definition.
     * @param windowDefinition the LogicalWindowDescriptorPtr.
     * @param id: the id of the operator if not defined then next free operator id is used.
     * @return UnaryOperatorPtr
     */
    static LogicalUnaryOperatorPtr
    createWindowOperator(Windowing::LogicalWindowDescriptorPtr const& windowDefinition, OperatorId id = getNextOperatorId());

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
    static LogicalBinaryOperatorPtr
    createJoinOperator(const Join::LogicalJoinDescriptorPtr& joinDefinition, OperatorId id = getNextOperatorId());

    /// todo put in experimental namespace
    /**
    * @brief Create a specialized batch join operator.
    * @param id: the id of the operator if not defined then next free operator id is used.
    * @return BinaryOperator
    */
    static LogicalBinaryOperatorPtr createBatchJoinOperator(
        const Join::Experimental::LogicalBatchJoinDescriptorPtr& batchJoinDefinition, OperatorId id = getNextOperatorId());
};

} /// namespace NES
