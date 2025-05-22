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

#include <algorithm>
#include <atomic>
#include <memory>
#include <numeric>
#include <sstream>
#include <utility>
#include <vector>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES::QueryCompilation
{

PipelineId getNextPipelineId()
{
    static std::atomic_uint64_t id = INITIAL_PIPELINE_ID.getRawValue();
    return PipelineId(id++);
}

OperatorPipeline::OperatorPipeline(PipelineId pipelineId, Type pipelineType)
    : id(pipelineId)
    , decomposedQueryPlan(std::make_shared<DecomposedQueryPlan>(INVALID_QUERY_ID, INVALID_WORKER_NODE_ID))
    , pipelineType(pipelineType)
{
}

std::shared_ptr<OperatorPipeline> OperatorPipeline::create()
{
    return std::make_shared<OperatorPipeline>(OperatorPipeline(getNextPipelineId(), Type::OperatorPipelineType));
}

std::shared_ptr<OperatorPipeline> OperatorPipeline::createSinkPipeline()
{
    return std::make_shared<OperatorPipeline>(OperatorPipeline(getNextPipelineId(), Type::SinkPipelineType));
}

void OperatorPipeline::setType(Type pipelineType)
{
    this->pipelineType = pipelineType;
}

bool OperatorPipeline::isOperatorPipeline() const
{
    return pipelineType == Type::OperatorPipelineType;
}

bool OperatorPipeline::isSinkPipeline() const
{
    return pipelineType == Type::SinkPipelineType;
}

bool OperatorPipeline::isSourcePipeline() const
{
    return pipelineType == Type::SourcePipelineType;
}

void OperatorPipeline::addPredecessor(const std::shared_ptr<OperatorPipeline>& pipeline)
{
    pipeline->successorPipelines.emplace_back(shared_from_this());
    this->predecessorPipelines.emplace_back(pipeline);
}

void OperatorPipeline::addSuccessor(const std::shared_ptr<OperatorPipeline>& pipeline)
{
    if (pipeline)
    {
        pipeline->predecessorPipelines.emplace_back(weak_from_this());
        this->successorPipelines.emplace_back(pipeline);
    }
}

std::vector<std::shared_ptr<OperatorPipeline>> OperatorPipeline::getPredecessors() const
{
    std::vector<std::shared_ptr<OperatorPipeline>> predecessors;
    predecessors.reserve(predecessorPipelines.size());
    for (const auto& predecessor : predecessorPipelines)
    {
        predecessors.emplace_back(predecessor.lock());
    }
    return predecessors;
}

bool OperatorPipeline::hasOperators() const
{
    return !this->decomposedQueryPlan->getRootOperators().empty();
}

void OperatorPipeline::clearPredecessors()
{
    for (const auto& pre : predecessorPipelines)
    {
        if (const auto prePipeline = pre.lock())
        {
            prePipeline->removeSuccessor(shared_from_this());
        }
    }
    predecessorPipelines.clear();
}

void OperatorPipeline::removePredecessor(const std::shared_ptr<OperatorPipeline>& pipeline)
{
    for (auto iter = predecessorPipelines.begin(); iter != predecessorPipelines.end(); ++iter)
    {
        if (iter->lock().get() == pipeline.get())
        {
            predecessorPipelines.erase(iter);
            return;
        }
    }
}

void OperatorPipeline::removeSuccessor(const std::shared_ptr<OperatorPipeline>& pipeline)
{
    for (auto iter = successorPipelines.begin(); iter != successorPipelines.end(); ++iter)
    {
        if (iter->get() == pipeline.get())
        {
            successorPipelines.erase(iter);
            return;
        }
    }
}

void OperatorPipeline::clearSuccessors()
{
    for (const auto& succ : successorPipelines)
    {
        succ->removePredecessor(shared_from_this());
    }
    successorPipelines.clear();
}

const std::vector<std::shared_ptr<OperatorPipeline>>& OperatorPipeline::getSuccessors() const
{
    return successorPipelines;
}

void OperatorPipeline::prependOperator(std::shared_ptr<Operator> newRootOperator)
{
    PRECONDITION(this->isOperatorPipeline() || !this->hasOperators(), "Sink and Source pipelines can have more then one operator");
    if (newRootOperator->hasProperty("LogicalOperatorId"))
    {
        operatorIds.push_back(std::any_cast<OperatorId>(newRootOperator->getProperty("LogicalOperatorId")));
    }
    this->decomposedQueryPlan->appendOperatorAsNewRoot(std::move(newRootOperator));
}

PipelineId OperatorPipeline::getPipelineId() const
{
    return id;
}

std::shared_ptr<DecomposedQueryPlan> OperatorPipeline::getDecomposedQueryPlan()
{
    return decomposedQueryPlan;
}

const std::vector<OperatorId>& OperatorPipeline::getOperatorIds() const
{
    return operatorIds;
}

template <typename T>
static std::vector<PipelineId> getIds(const std::vector<T>& pipelines)
{
    std::vector<PipelineId> ids;
    std::transform(
        pipelines.begin(), pipelines.end(), std::back_inserter(ids), [](const auto& pipeline) { return pipeline.get()->getPipelineId(); });
    return ids;
}

std::string OperatorPipeline::toString() const
{
    auto successorsStr = std::accumulate(
        successorPipelines.begin(),
        successorPipelines.end(),
        std::string(),
        [](const std::string& result, const std::shared_ptr<OperatorPipeline>& succPipeline)
        {
            auto succPipelineId = fmt::format("{}", succPipeline->id);
            return result.empty() ? succPipelineId : result + ", " + succPipelineId;
        });

    auto predecessorsStr = std::accumulate(
        predecessorPipelines.begin(),
        predecessorPipelines.end(),
        std::string(),
        [](const std::string& result, const std::weak_ptr<OperatorPipeline>& predecessorPipeline)
        {
            auto predecessorPipelineId = fmt::format("{}", predecessorPipeline.lock()->id);
            return result.empty() ? predecessorPipelineId : result + ", " + predecessorPipelineId;
        });

    return fmt::format(
        "- Id: {}, Type: {}, Successors: {}, Predecessors: {}\n- QueryPlan: {}",
        id,
        magic_enum::enum_name(pipelineType),
        successorsStr,
        predecessorsStr,
        decomposedQueryPlan->toString());
}
}
