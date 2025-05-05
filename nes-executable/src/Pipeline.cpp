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

#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <ExecutablePipelineStage.hpp>
#include <Pipeline.hpp>

namespace NES
{

namespace
{
std::string operatorChainToString(const PhysicalOperator& op, int indent = 0)
{
    std::string indentation(indent, ' ');
    std::string result = fmt::format("{}{}\n", indentation, op.toString());
    if (auto childOpt = op.getChild())
    {
        result += operatorChainToString(childOpt.value(), indent + 2);
    }
    return result;
}

std::string pipelineToString(const Pipeline& pipeline, uint16_t indent)
{
    fmt::memory_buffer buf;
    auto indentStr = std::string(indent, ' ');

    fmt::format_to(
        std::back_inserter(buf),
        "{}Pipeline(ID({}), Provider({}))\n",
        indentStr,
        pipeline.pipelineId.getRawValue(),
        magic_enum::enum_name(pipeline.providerType)
    );

    fmt::format_to(
        std::back_inserter(buf),
        "{}  Operator chain:\n{}",
        indentStr,
        operatorChainToString(pipeline.rootOperator, indent + 4)
    );

    for (auto& succ : pipeline.getSuccessors()) {
        fmt::format_to(std::back_inserter(buf), "{}  Successor Pipeline:\n", indentStr);
        fmt::format_to(std::back_inserter(buf), "{}", pipelineToString(*succ, indent + 4));
    }
    return fmt::to_string(buf);
}

std::atomic_uint64_t nextId{ INITIAL_PIPELINE_ID.getRawValue() };
}

PipelineId getNextPipelineId() {
    return PipelineId(nextId++);
}

Pipeline::Pipeline(PhysicalOperator op) : rootOperator(std::move(op)), pipelineId(getNextPipelineId())
{
}

Pipeline::Pipeline(SourcePhysicalOperator op) : rootOperator(std::move(op)), pipelineId(getNextPipelineId())
{
}

Pipeline::Pipeline(SinkPhysicalOperator op) : rootOperator(std::move(op)), pipelineId(getNextPipelineId())
{
}

bool Pipeline::isSourcePipeline() const
{
    return rootOperator.tryGet<SourcePhysicalOperator>().has_value();
}

bool Pipeline::isOperatorPipeline() const
{
    return not(isSinkPipeline() or isSourcePipeline());
}

bool Pipeline::isSinkPipeline() const
{
    return rootOperator.tryGet<SinkPhysicalOperator>().has_value();
}

void Pipeline::prependOperator(PhysicalOperator newOp)
{
    PRECONDITION(not(isSourcePipeline() or isSinkPipeline()), "Cannot add new operator to source or sink pipeline");
    newOp.setChild(rootOperator);
    rootOperator = newOp;
}

PhysicalOperator appendOperatorHelper(PhysicalOperator op, const PhysicalOperator& newOp)
{
    if (not op.getChild())
    {
        op.setChild(newOp);
        return op;
    }
    PhysicalOperator child = op.getChild().value();
    child = appendOperatorHelper(child, newOp);
    op.setChild(child);
    return op;
}

void Pipeline::appendOperator(PhysicalOperator newOp)
{
    PRECONDITION(not(isSourcePipeline() or isSinkPipeline()), "Cannot add new operator to source or sink pipeline");
    rootOperator = appendOperatorHelper(rootOperator, newOp);
}

void Pipeline::addSuccessor(const std::shared_ptr<Pipeline>& successor, const std::weak_ptr<Pipeline>& self)
{
    if (successor)
    {
        successor->predecessorPipelines.emplace_back(self);
        this->successorPipelines.emplace_back(successor);
    }
}

void Pipeline::removePredecessor(const Pipeline& pipeline)
{
    for (auto iter = predecessorPipelines.begin(); iter != predecessorPipelines.end(); ++iter)
    {
        if (iter->lock()->pipelineId == pipeline.pipelineId)
        {
            predecessorPipelines.erase(iter);
            return;
        }
    }
}

const std::vector<std::shared_ptr<Pipeline>>& Pipeline::getSuccessors() const
{
    return successorPipelines;
}

void Pipeline::clearSuccessors() {
    for (const auto& succ : successorPipelines)
    {
        succ->removePredecessor(*this);
    }
    successorPipelines.clear();
}

void Pipeline::clearPredecessors()
{
    for (const auto& pre : predecessorPipelines)
    {
        if (const auto prePipeline = pre.lock())
        {
            prePipeline->removeSuccessor(*this);
        }
    }
    predecessorPipelines.clear();
}

void Pipeline::removeSuccessor(const Pipeline& pipeline)
{
    for (auto iter = successorPipelines.begin(); iter != successorPipelines.end(); ++iter)
    {
        if (iter->get()->pipelineId == pipeline.pipelineId)
        {
            successorPipelines.erase(iter);
            return;
        }
    }
}

std::ostream& operator<<(std::ostream& os, const Pipeline& p)
{
    os << pipelineToString(p, 0);
    return os;
}
}
