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
std::string operatorChainToString(const NES::PhysicalOperator& op, int indent = 0) {
    std::ostringstream oss;
    std::string indentation(indent, ' ');
    oss << indentation << op.toString() << "\n";
    if (auto childOpt = op.getChild()) {
        oss << operatorChainToString(childOpt.value(), indent + 2);
    }
    return oss.str();
}

PipelineId getNextPipelineId() {
    static std::atomic_uint64_t id = NES::INITIAL_PIPELINE_ID.getRawValue();
    return PipelineId(id++);
}
}


std::unordered_map<uint64_t, std::shared_ptr<OperatorHandler>> Pipeline::releaseOperatorHandlers()
{
    return std::move(operatorHandlers);
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

bool Pipeline::isSourcePipeline()
{
    return rootOperator.tryGet<SourcePhysicalOperator>();
}

bool Pipeline::isOperatorPipeline()
{
    return not (isSinkPipeline() or isSourcePipeline());
}

bool Pipeline::isSinkPipeline()
{
    return rootOperator.tryGet<SinkPhysicalOperator>();
}

void Pipeline::prependOperator(PhysicalOperator newOp) {
    PRECONDITION(not (isSourcePipeline() or isSinkPipeline()), "Cannot add new operator to source or sink pipeline");
    newOp.setChild(rootOperator);
    rootOperator = newOp;
}

PhysicalOperator appendOperatorHelper(PhysicalOperator op, const PhysicalOperator& newOp) {
    if (not op.getChild()) {
        op.setChild(newOp);
        return op;
    } else {
        PhysicalOperator child = op.getChild().value();
        child = appendOperatorHelper(child, newOp);
        op.setChild(child);
        return op;
    }
}

void Pipeline::appendOperator(PhysicalOperator newOp) {
    PRECONDITION(not (isSourcePipeline() or isSinkPipeline()), "Cannot add new operator to source or sink pipeline");
    rootOperator = appendOperatorHelper(rootOperator, newOp);
}

std::string Pipeline::toString() const {
    std::ostringstream oss;
    oss << "PipelineId: " << pipelineId.getRawValue() << "\n";
    oss << "Provider: " << magic_enum::enum_name(providerType) << "\n";
    oss << "Successors: " << successorPipelines.size() << "\n";
    oss << "Predecessors: " << predecessorPipelines.size() << "\n";
    oss << "Operator Chain:\n";
    oss << operatorChainToString(rootOperator);
    return oss.str();
}

std::ostream& operator<<(std::ostream& os, const Pipeline& p) {
    os << p.toString();
    return os;
}
}