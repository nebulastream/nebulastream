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


#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/RenameSourceOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Util/Placement/PlacementConstants.hpp>

namespace NES
{

std::shared_ptr<LogicalUnaryOperator> LogicalOperatorFactory::createSourceOperator(std::string logicalSourceName, OperatorId id)
{
    return std::make_shared<SourceNameLogicalOperator>(std::move(logicalSourceName), id);
}

std::shared_ptr<LogicalUnaryOperator> LogicalOperatorFactory::createSourceOperator(
    std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor, OperatorId id, OriginId originId)
{
    return std::make_shared<SourceDescriptorLogicalOperator>(std::move(sourceDescriptor), id, originId);
}

std::shared_ptr<LogicalUnaryOperator> LogicalOperatorFactory::createSinkOperator(WorkerId workerId, OperatorId id)
{
    auto sinkOperator = std::make_shared<SinkLogicalOperator>(id);
    if (workerId != INVALID_WORKER_NODE_ID)
    {
        sinkOperator->addProperty(Optimizer::PINNED_WORKER_ID, workerId);
    }
    return sinkOperator;
}

std::shared_ptr<LogicalUnaryOperator> LogicalOperatorFactory::createSinkOperator(std::string sinkName, WorkerId workerId, OperatorId id)
{
    auto sinkOperator = std::make_shared<SinkLogicalOperator>(std::move(sinkName), id);
    if (workerId != INVALID_WORKER_NODE_ID)
    {
        sinkOperator->addProperty(Optimizer::PINNED_WORKER_ID, workerId);
    }
    return sinkOperator;
}

std::shared_ptr<LogicalUnaryOperator>
LogicalOperatorFactory::createFilterOperator(const std::shared_ptr<LogicalFunction>& predicate, OperatorId id)
{
    return std::make_shared<LogicalSelectionOperator>(predicate, id);
}

std::shared_ptr<LogicalUnaryOperator> LogicalOperatorFactory::createRenameSourceOperator(const std::string& newSourceName, OperatorId id)
{
    return std::make_shared<RenameSourceOperator>(newSourceName, id);
}

std::shared_ptr<LogicalUnaryOperator> LogicalOperatorFactory::createLimitOperator(const uint64_t limit, OperatorId id)
{
    return std::make_shared<LogicalLimitOperator>(limit, id);
}

std::shared_ptr<LogicalUnaryOperator>
LogicalOperatorFactory::createProjectionOperator(const std::vector<std::shared_ptr<LogicalFunction>>& functions, OperatorId id)
{
    return std::make_shared<LogicalProjectionOperator>(functions, id);
}

std::shared_ptr<LogicalUnaryOperator>
LogicalOperatorFactory::createMapOperator(const std::shared_ptr<FieldAssignmentBinaryLogicalFunction>& mapFunction, OperatorId id)
{
    return std::make_shared<LogicalMapOperator>(mapFunction, id);
}

std::shared_ptr<LogicalUnaryOperator> LogicalOperatorFactory::createInferModelOperator(
    std::string model,
    std::vector<std::shared_ptr<LogicalFunction>> inputFieldsPtr,
    std::vector<std::shared_ptr<LogicalFunction>> outputFieldsPtr,
    OperatorId id)
{
    return std::make_shared<NES::InferModel::LogicalInferModelOperator>(model, inputFieldsPtr, outputFieldsPtr, id);
}

std::shared_ptr<LogicalBinaryOperator> LogicalOperatorFactory::createUnionOperator(OperatorId id)
{
    return std::make_shared<LogicalUnionOperator>(id);
}

std::shared_ptr<LogicalBinaryOperator>
LogicalOperatorFactory::createJoinOperator(const std::shared_ptr<Join::LogicalJoinDescriptor>& joinDefinition, OperatorId id)
{
    return std::make_shared<LogicalJoinOperator>(joinDefinition, id);
}

std::shared_ptr<LogicalUnaryOperator>
LogicalOperatorFactory::createWindowOperator(const std::shared_ptr<Windowing::LogicalWindowDescriptor>& windowDefinition, OperatorId id)
{
    return std::make_shared<LogicalWindowOperator>(windowDefinition, id);
}

std::shared_ptr<LogicalUnaryOperator> LogicalOperatorFactory::createWatermarkAssignerOperator(
    const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor, OperatorId id)
{
    return std::make_shared<WatermarkAssignerLogicalOperator>(watermarkStrategyDescriptor, id);
}

}
