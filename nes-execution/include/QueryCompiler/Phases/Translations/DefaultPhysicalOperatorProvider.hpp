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

#include <memory>
#include <string>
#include <tuple>
#include <vector>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <QueryCompiler/Phases/Translations/PhysicalOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Types/TimeBasedWindowType.hpp>

namespace NES::QueryCompilation
{

struct WindowOperatorProperties
{
    WindowOperatorProperties(
        WindowOperatorPtr windowOperator,
        SchemaPtr windowInputSchema,
        SchemaPtr windowOutputSchema,
        Windowing::LogicalWindowDescriptorPtr windowDefinition)
        : windowOperator(std::move(windowOperator))
        , windowInputSchema(std::move(windowInputSchema))
        , windowOutputSchema(std::move(windowOutputSchema))
        , windowDefinition(std::move(windowDefinition)) {};

    WindowOperatorPtr windowOperator;
    SchemaPtr windowInputSchema;
    SchemaPtr windowOutputSchema;
    Windowing::LogicalWindowDescriptorPtr windowDefinition;
};

/// All operator nodes for lowering the stream joins
struct StreamJoinOperators
{
    StreamJoinOperators(const LogicalOperatorPtr& operatorNode, const OperatorPtr& leftInputOperator, const OperatorPtr& rightInputOperator)
        : operatorNode(operatorNode), leftInputOperator(leftInputOperator), rightInputOperator(rightInputOperator)
    {
    }
    const LogicalOperatorPtr& operatorNode;
    const OperatorPtr& leftInputOperator;
    const OperatorPtr& rightInputOperator;
};

struct StreamJoinConfigs
{
    StreamJoinConfigs(
        const std::vector<std::string>& joinFieldNamesLeft,
        const std::vector<std::string>& joinFieldNamesRight,
        const uint64_t windowSize,
        const uint64_t windowSlide,
        const TimestampField& timeStampFieldLeft,
        const TimestampField& timeStampFieldRight,
        const StreamJoinStrategy& joinStrategy)
        : joinFieldNamesLeft(joinFieldNamesLeft)
        , joinFieldNamesRight(joinFieldNamesRight)
        , windowSize(windowSize)
        , windowSlide(windowSlide)
        , timeStampFieldLeft(timeStampFieldLeft)
        , timeStampFieldRight(timeStampFieldRight)
        , joinStrategy(joinStrategy)
    {
    }
    std::vector<std::string> joinFieldNamesLeft;
    std::vector<std::string> joinFieldNamesRight;
    const uint64_t windowSize;
    const uint64_t windowSlide;
    const TimestampField& timeStampFieldLeft;
    const TimestampField& timeStampFieldRight;
    const StreamJoinStrategy& joinStrategy;
};

/// Provides a set of default lowerings for logical operators to corresponding physical operators.
class DefaultPhysicalOperatorProvider : public PhysicalOperatorProvider
{
public:
    explicit DefaultPhysicalOperatorProvider(std::shared_ptr<QueryCompilerOptions> options);
    ~DefaultPhysicalOperatorProvider() noexcept override = default;

    void lower(DecomposedQueryPlanPtr decomposedQueryPlan, LogicalOperatorPtr operatorNode) override;

protected:
    void insertDemultiplexOperatorsBefore(const LogicalOperatorPtr& operatorNode);
    void insertMultiplexOperatorsAfter(const LogicalOperatorPtr& operatorNode);

    /// Checks if the current operator is a demultiplexer, if it has multiple parents.
    bool isDemultiplex(const LogicalOperatorPtr& operatorNode);

    void lowerBinaryOperator(const LogicalOperatorPtr& operatorNode);
    void lowerUnaryOperator(const LogicalOperatorPtr& operatorNode);

    /// Lowers a union operator. However, A Union operator is not realized via executable code. It is realized by
    /// using a Multiplex operation that connects two sources with one sink. The two sources then form one stream
    /// that continuously sends TupleBuffers to the sink. This means a query that only contains an Union operator
    /// does not lead to code that is compiled and is entirely executed on the source/sink/TupleBuffer level.
    void lowerUnionOperator(const LogicalOperatorPtr& operatorNode);

    void lowerProjectOperator(const LogicalOperatorPtr& operatorNode);

    void lowerInferModelOperator(LogicalOperatorPtr operatorNode);

    void lowerMapOperator(const LogicalOperatorPtr& operatorNode);

    void lowerWindowOperator(const LogicalOperatorPtr& operatorNode);

    void lowerTimeBasedWindowOperator(const LogicalOperatorPtr& operatorNode);

    void lowerWatermarkAssignmentOperator(const LogicalOperatorPtr& operatorNode);

    void lowerJoinOperator(const LogicalOperatorPtr& operatorNode);

    void lowerSortBufferOperator(const LogicalOperatorPtr& operatorNode);

    void lowerDelayBufferOperator(const LogicalOperatorPtr& operatorNode);

    OperatorPtr getJoinBuildInputOperator(const LogicalJoinOperatorPtr& joinOperator, SchemaPtr schema, std::vector<OperatorPtr> children);

private:
    /// Lowers the stream nested loop join
    std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler>
    lowerStreamingNestedLoopJoin(const StreamJoinOperators& streamJoinOperators, const StreamJoinConfigs& streamJoinConfig);

    /// replaces the window sink (and inserts a SliceStoreAppendOperator) depending on the time based window type for keyed windows
    [[nodiscard]] std::shared_ptr<Node>
    replaceOperatorTimeBasedWindow(WindowOperatorProperties& windowOperatorProperties, const LogicalOperatorPtr& operatorNode);

    [[nodiscard]] static std::tuple<TimestampField, TimestampField>
    getTimestampLeftAndRight(const std::shared_ptr<LogicalJoinOperator>& joinOperator, const Windowing::TimeBasedWindowTypePtr& windowType);
};
}
