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
#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Configurations/Enums/CompilationStrategy.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/Phases/Translations/PhysicalOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <Types/TimeBasedWindowType.hpp>

namespace NES::QueryCompilation
{

struct WindowOperatorProperties
{
    WindowOperatorProperties(
        std::shared_ptr<WindowOperator> windowOperator,
        std::shared_ptr<Schema> windowInputSchema,
        std::shared_ptr<Schema> windowOutputSchema,
        std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition)
        : windowOperator(std::move(windowOperator))
        , windowInputSchema(std::move(windowInputSchema))
        , windowOutputSchema(std::move(windowOutputSchema))
        , windowDefinition(std::move(windowDefinition)) { };

    std::shared_ptr<WindowOperator> windowOperator;
    std::shared_ptr<Schema> windowInputSchema;
    std::shared_ptr<Schema> windowOutputSchema;
    std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition;
};

/// All operator nodes for lowering the stream joins
struct StreamJoinOperators
{
    StreamJoinOperators(
        const std::shared_ptr<LogicalOperator>& operatorNode,
        const std::shared_ptr<Operator>& leftInputOperator,
        const std::shared_ptr<Operator>& rightInputOperator)
        : operatorNode(operatorNode), leftInputOperator(leftInputOperator), rightInputOperator(rightInputOperator)
    {
    }

    std::shared_ptr<LogicalOperator> operatorNode;
    std::shared_ptr<Operator> leftInputOperator;
    std::shared_ptr<Operator> rightInputOperator;
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
        const Configurations::StreamJoinStrategy& joinStrategy)
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
    const Configurations::StreamJoinStrategy& joinStrategy;
};

/// Provides a set of default lowerings for logical operators to corresponding physical operators.
class DefaultPhysicalOperatorProvider : public PhysicalOperatorProvider
{
public:
    explicit DefaultPhysicalOperatorProvider(Configurations::QueryCompilerConfiguration queryCompilerConfig);
    ~DefaultPhysicalOperatorProvider() noexcept override = default;

    void lower(const DecomposedQueryPlan& decomposedQueryPlan, std::shared_ptr<LogicalOperator> operatorNode) override;

protected:
    static void insertDemultiplexOperatorsBefore(const std::shared_ptr<LogicalOperator>& operatorNode);
    static void insertMultiplexOperatorsAfter(const std::shared_ptr<LogicalOperator>& operatorNode);

    /// Checks if the current operator is a demultiplexer, if it has multiple parents.
    static bool isDemultiplex(const std::shared_ptr<LogicalOperator>& operatorNode);

    void lowerBinaryOperator(const std::shared_ptr<LogicalOperator>& operatorNode);
    static void lowerUnaryOperator(const std::shared_ptr<LogicalOperator>& operatorNode);
    static void lowerProjectOperator(const std::shared_ptr<LogicalOperator>& operatorNode);
    static void lowerMapOperator(const std::shared_ptr<LogicalOperator>& operatorNode);
    static void lowerWindowOperator(const std::shared_ptr<LogicalOperator>& operatorNode);
    static void lowerTimeBasedWindowOperator(const std::shared_ptr<LogicalOperator>& operatorNode);
    static void lowerWatermarkAssignmentOperator(const std::shared_ptr<LogicalOperator>& operatorNode);
    void lowerJoinOperator(const std::shared_ptr<LogicalOperator>& operatorNode);

    /// Lowers a union operator. However, A Union operator is not realized via executable code. It is realized by
    /// using a Multiplex operation that connects two sources with one sink. The two sources then form one stream
    /// that continuously sends TupleBuffersstatic  to the sink. This means a query that only contains an Union operator
    /// does not lead to code that is compiled and is entirely executed on the source/sink/TupleBuffer level.
    static void lowerUnionOperator(const std::shared_ptr<LogicalOperator>& operatorNode);


    std::shared_ptr<Operator> getJoinBuildInputOperator(
        const std::shared_ptr<LogicalJoinOperator>& joinOperator,
        const std::shared_ptr<Schema>& outputSchema,
        std::vector<std::shared_ptr<Operator>> children);

private:
    /// Lowers the stream nested loop join
    std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler>
    lowerStreamingNestedLoopJoin(const StreamJoinOperators& streamJoinOperators, const StreamJoinConfigs& streamJoinConfig) const;

    /// replaces the window sink (and inserts a SliceStoreAppendOperator) depending on the time based window type for keyed windows
    [[nodiscard]] std::shared_ptr<Node> replaceOperatorTimeBasedWindow(
        WindowOperatorProperties& windowOperatorProperties, const std::shared_ptr<LogicalOperator>& operatorNode);

    [[nodiscard]] static std::tuple<TimestampField, TimestampField> getTimestampLeftAndRight(
        const std::shared_ptr<LogicalJoinOperator>& joinOperator, const std::shared_ptr<Windowing::TimeBasedWindowType>& windowType);
};
}
