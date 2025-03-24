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

#include <memory>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMemoryLayoutSwapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/MemoryLayoutSelection/FixMemoryLayoutSelectionPhase.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>


namespace NES::QueryCompilation
{

std::shared_ptr<MemoryLayoutSelectionPhase> FixMemoryLayoutSelectionPhase::create(Schema::MemoryLayoutType layoutType)
{
    return std::make_shared<FixMemoryLayoutSelectionPhase>(layoutType);
}

FixMemoryLayoutSelectionPhase::FixMemoryLayoutSelectionPhase(Schema::MemoryLayoutType layoutType) : layoutType(layoutType)
{
}

std::shared_ptr<PipelineQueryPlan> FixMemoryLayoutSelectionPhase::apply(std::shared_ptr<PipelineQueryPlan> pipelineQueryPlan)
{
    if (pipelineQueryPlan->getPipelines().empty())
    {
        NES_WARNING("FixMemoryLayoutSelectionPhase::apply called on empty pipeline query plan");
        return pipelineQueryPlan;
    }
    auto pipelines = pipelineQueryPlan->getPipelines();
    for (const auto& pipeline : pipelines)
    {
        if (not pipeline)
        {
            continue;
        }
        if (pipeline->isOperatorPipeline())
        {
            process(pipeline);
        }
    }
    for (const auto& pipeline : pipelines)
    {
        if (not pipeline)
        {
            continue;
        }
        std::shared_ptr<OperatorPipeline> newPipeline;
        /// For sink pipelines: compare last operator of predecessor pipeline
        if (pipeline->isSinkPipeline())
        {
            insertBetweenThisAndPredecessor(pipeline, newPipeline);
            /// emtpy if no swap is needed
            if (newPipeline)
            {
                pipelineQueryPlan->addPipeline(newPipeline);
            }
        }
        /// For source pipelines: compare first operator of successor pipeline
        else if (pipeline->isSourcePipeline())
        {
            insertBetweenThisAndSuccessor(pipeline, newPipeline);
            if (newPipeline)
            {
                pipelineQueryPlan->addPipeline(newPipeline);
            }
        }
    }
    return pipelineQueryPlan;
}

void FixMemoryLayoutSelectionPhase::process(std::shared_ptr<OperatorPipeline> pipeline)
{
    const auto decomposedQueryPlan = pipeline->getDecomposedQueryPlan();
    const auto pipelineRootOperators = decomposedQueryPlan->getRootOperators();
    PRECONDITION(not pipelineRootOperators.empty(), "A pipeline should have at least one root operator");

    for (auto& rootOperator : pipelineRootOperators)
    {
        processOperator(rootOperator);
    }
}

void FixMemoryLayoutSelectionPhase::processOperator(const std::shared_ptr<Operator> op)
{
    auto schema = op->getOutputSchema();
    schema->setLayoutType(layoutType);

    if (NES::Util::instanceOf<PhysicalOperators::PhysicalUnaryOperator>(op))
    {
        auto unaryOp = NES::Util::as<PhysicalOperators::PhysicalUnaryOperator>(op);
        auto inputSchema = unaryOp->getInputSchema();
        inputSchema->setLayoutType(layoutType);
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalBinaryOperator>(op))
    {
        auto binaryOp = NES::Util::as<PhysicalOperators::PhysicalBinaryOperator>(op);
        auto leftInputSchema = binaryOp->getLeftInputSchema();
        auto rightInputSchema = binaryOp->getRightInputSchema();

        leftInputSchema->setLayoutType(layoutType);
        rightInputSchema->setLayoutType(layoutType);
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalScanOperator>(op))
    {
        auto scanOp = NES::Util::as<PhysicalOperators::PhysicalScanOperator>(op);
        auto inputSchema = scanOp->getInputSchema();
        inputSchema->setLayoutType(layoutType);
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalEmitOperator>(op))
    {
        auto emitOp = NES::Util::as<PhysicalOperators::PhysicalEmitOperator>(op);
        auto inputSchema = emitOp->getInputSchema();
        inputSchema->setLayoutType(layoutType);
    }
    else if (NES::Util::instanceOf<LogicalUnaryOperator>(op))
    {
        auto unaryOp = NES::Util::as<LogicalUnaryOperator>(op);
        auto inputSchema = unaryOp->getInputSchema();
        inputSchema->setLayoutType(layoutType);
    }

    if (!op.get()->getChildren().empty())
    {
        for (const auto& child : op->getChildren())
        {
            /// Assuming that each child can be treated as an Operator.
            processOperator(NES::Util::as<Operator>(child));
        }
    }
}
void FixMemoryLayoutSelectionPhase::insertBetweenThisAndSuccessor(
    const std::shared_ptr<OperatorPipeline>& pipeline, std::shared_ptr<OperatorPipeline>& newPipeline)
{
    /// Assumption: source pipelines consist only of a single source operator.
    /// Additionally, the next operator is the first operator of the successor pipeline.

    auto source = Util::as<LogicalUnaryOperator>(pipeline->getDecomposedQueryPlan()->getRootOperators()[0]);
    auto successorPipelines = pipeline->getSuccessors();
    if (successorPipelines.empty() || successorPipelines[0]->getDecomposedQueryPlan()->getRootOperators().empty())
    {
        return;
    }

    auto succOp = successorPipelines[0]->getDecomposedQueryPlan()->getRootOperators()[0];
    auto inputSchema = source->getOutputSchema()->copy();
    auto outputSchema = getInputSchema(succOp)->copy();

    if (inputSchema->getLayoutType() == outputSchema->getLayoutType())
    {
        return;
    }

    /// insert swap pipeline after source pipeline
    newPipeline = OperatorPipeline::create();
    auto newOperator = PhysicalOperators::PhysicalMemoryLayoutSwapOperator::create(inputSchema, outputSchema);
    newPipeline->prependOperator(newOperator);

    /// Splice newPipeline between pipeline and its successors.
    auto successors = pipeline->getSuccessors();
    pipeline->clearSuccessors();
    pipeline->addSuccessor(newPipeline);
    for (const auto& succ : successors)
    {
        newPipeline->addSuccessor(succ);
        succ->removePredecessor(pipeline);
    }
}
void FixMemoryLayoutSelectionPhase::insertBetweenThisAndPredecessor(
    const std::shared_ptr<OperatorPipeline>& pipeline, std::shared_ptr<OperatorPipeline>& newPipeline)
{
    /// Assumption: sink pipelines consist only of a single sink operator.
    /// Additionally, the next operator is the first operator of the successor pipeline.

    auto sinkOp = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto predPipelines = pipeline->getPredecessors();

    if (predPipelines.empty() || predPipelines[0]->getDecomposedQueryPlan()->getLeafOperators().empty())
        return;

    auto predOp = predPipelines[0]->getDecomposedQueryPlan()->getLeafOperators()[0];
    auto outputSchema = getInputSchema(sinkOp)->copy();
    auto inputSchema = predOp->getOutputSchema()->copy();

    if (inputSchema->getLayoutType() == outputSchema->getLayoutType()) /// no swap needed
        return;
    /// insert swap pipeline after source pipeline
    newPipeline = OperatorPipeline::create();
    auto newOperator = PhysicalOperators::PhysicalMemoryLayoutSwapOperator::create(inputSchema, outputSchema);
    newPipeline->prependOperator(newOperator);

    /// Splice newPipeline between pipeline and its successors.
    auto predecessors = pipeline->getPredecessors();
    pipeline->clearPredecessors();
    pipeline->addPredecessor(newPipeline);
    for (const auto& pred : predecessors)
    {
        newPipeline->addPredecessor(pred);
        pred->removeSuccessor(pipeline);
    }
}
std::shared_ptr<Schema> FixMemoryLayoutSelectionPhase::getInputSchema(const std::shared_ptr<Operator> op)
{
    if (NES::Util::instanceOf<PhysicalOperators::PhysicalUnaryOperator>(op))
    {
        auto unaryOp = NES::Util::as<PhysicalOperators::PhysicalUnaryOperator>(op);
        return unaryOp->getInputSchema();
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalBinaryOperator>(op))
    {
        auto binaryOp = NES::Util::as<PhysicalOperators::PhysicalBinaryOperator>(op);
        return binaryOp->getLeftInputSchema();
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalScanOperator>(op))
    {
        auto scanOp = NES::Util::as<PhysicalOperators::PhysicalScanOperator>(op);
        return scanOp->getInputSchema();
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalEmitOperator>(op))
    {
        auto emitOp = NES::Util::as<PhysicalOperators::PhysicalEmitOperator>(op);
        return emitOp->getInputSchema();
    }
    else if (NES::Util::instanceOf<LogicalUnaryOperator>(op))
    {
        auto unaryOp = NES::Util::as<LogicalUnaryOperator>(op);
        return unaryOp->getInputSchema();
    }
    else
    { /// failsafe
        return op->getOutputSchema()->copy();
    }
}
}
