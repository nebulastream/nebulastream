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
#include <utility>
#include <variant>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Phases/PipeliningPhase.hpp>
#include <Plans/QueryPlan.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <Plans/Operator.hpp>
#include <Abstract/PhysicalOperator.hpp>

namespace NES::QueryCompilation::PipeliningPhase
{

Pipeline::PipelineOperator convertOperator(std::unique_ptr<Operator> op) {
    if (auto phys = dynamic_cast<PhysicalOperator*>(op.get())) {
        /// Release ownership and convert using dynamic_cast.
        PhysicalOperator* rawPtr = dynamic_cast<PhysicalOperator*>(op.release());
        INVARIANT(rawPtr, "Conversion to PhysicalOperator failed");
        return Pipeline::PipelineOperator(std::unique_ptr<PhysicalOperator>(rawPtr));
    } else if (auto src = dynamic_cast<PhysicalOperatorWithSchema*>(op.get())) {
        PhysicalOperatorWithSchema* rawPtr = dynamic_cast<PhysicalOperatorWithSchema*>(op.release());
        INVARIANT(rawPtr, "Conversion to PhysicalOperatorWithSchema failed");
        return Pipeline::PipelineOperator(std::unique_ptr<PhysicalOperatorWithSchema>(rawPtr));
    } else if (auto src = dynamic_cast<SourceDescriptorLogicalOperator*>(op.get())) {
        SourceDescriptorLogicalOperator* rawPtr = dynamic_cast<SourceDescriptorLogicalOperator*>(op.release());
        INVARIANT(rawPtr, "Conversion to SourceDescriptorLogicalOperator failed");
        return Pipeline::PipelineOperator(std::unique_ptr<SourceDescriptorLogicalOperator>(rawPtr));
    } else if (auto sink = dynamic_cast<SinkLogicalOperator*>(op.get())) {
        SinkLogicalOperator* rawPtr = dynamic_cast<SinkLogicalOperator*>(op.release());
        INVARIANT(rawPtr, "Conversion to SinkLogicalOperator failed");
        return Pipeline::PipelineOperator(std::unique_ptr<SinkLogicalOperator>(rawPtr));
    } else {
        INVARIANT(false, "Operator is now pipeline operator: {}", typeid(op));
    }
}

/// The visitor is responsible for recursively building pipelines.
struct PipeliningVisitor {
    PipelinedQueryPlan& plan;
    Pipeline* currentPipeline; /// non-owning pointer

    void operator()(std::unique_ptr<PhysicalOperator>&) {
        /// Noop
    }

    /// Process SourceDescriptorLogicalOperator.
    void operator()(std::unique_ptr<SourceDescriptorLogicalOperator>& op) {
        if (currentPipeline->hasOperators()) {
            /// If current pipeline already has operators, create a new pipeline.
            auto newPipeline = std::make_unique<SourcePipeline>();
            newPipeline->prependOperator(Pipeline::PipelineOperator(std::move(op)));
            currentPipeline->successorPipelines.push_back(std::move(newPipeline));
            currentPipeline = currentPipeline->successorPipelines.back().get();
        } else {
            currentPipeline->prependOperator(Pipeline::PipelineOperator(std::move(op)));
        }
        auto* srcOp = static_cast<SourcePipeline*>(currentPipeline)->sourceOperator.get();
        /// Transfer ownership of children.
        for (auto& child : srcOp->releaseChildren()) {
            auto childVariant = convertOperator(std::move(child));
            std::visit(PipeliningVisitor{plan, currentPipeline}, childVariant);
        }
    }

    /// Process SinkLogicalOperator.
    void operator()(std::unique_ptr<SinkLogicalOperator>& op) {
        currentPipeline->prependOperator(Pipeline::PipelineOperator(std::move(op)));
        auto* sinkOp = static_cast<SinkPipeline*>(currentPipeline)->sinkOperator.get();
        /// For each child, create a new SinkPipeline.
        for (auto& child : sinkOp->releaseChildren()) {
            auto newPipeline = std::make_unique<SinkPipeline>();
            currentPipeline->successorPipelines.push_back(std::move(newPipeline));
            Pipeline* childPipeline = currentPipeline->successorPipelines.back().get();
            PipeliningVisitor childVisitor{plan, childPipeline};
            auto childVariant = convertOperator(std::move(child));
            std::visit(childVisitor, childVariant);
        }
    }

    void operator()(std::unique_ptr<PhysicalOperatorWithSchema>& op)
    {
        // noop
    }
    /// Process PhysicalOperator.
    void operator()(std::unique_ptr<PhysicalOperatorWithSchema>& op) {
        if (op->physicalOperator->isPipelineBreaker) {
            /// Pipeline breaker: add operator and create a new pipeline for each child.
            currentPipeline->prependOperator(Pipeline::PipelineOperator(std::move(op)));
            auto* physOp = static_cast<OperatorPipeline*>(currentPipeline)->rootOperator.get();
            for (auto& child : physOp->releaseChildren()) {
                auto newPipeline = std::make_unique<OperatorPipeline>();
                currentPipeline->successorPipelines.push_back(std::move(newPipeline));
                Pipeline* childPipeline = currentPipeline->successorPipelines.back().get();
                PipeliningVisitor childVisitor{plan, childPipeline};
                auto childVariant = convertOperator(std::move(child));
                std::visit(childVisitor, childVariant);
            }
        } else {
            /// Fusible operator: add operator and process children in the same pipeline.
            auto children = op->physicalOperator->releaseChildren();
            currentPipeline->prependOperator(Pipeline::PipelineOperator(std::move(op)));
            for (auto& child : children) {
                auto childVariant = convertOperator(std::move(child));
                std::visit(PipeliningVisitor{plan, currentPipeline}, childVariant);
            }
        }
    }
};


/// Entry point: splits the query plan of physical operators into pipelines
PipelinedQueryPlan apply(QueryPlan queryPlan) {
    PipelinedQueryPlan plan(queryPlan.getQueryId());
    for (auto& sinkOp : queryPlan.releaseRootOperators()) {
        auto pipeline = std::make_unique<SinkPipeline>();
        pipeline->prependOperator(convertOperator(std::move(sinkOp)));
        plan.pipelines.push_back(std::move(pipeline));
        SinkPipeline* sinkPipeline = static_cast<SinkPipeline*>(plan.pipelines.back().get());
        PipeliningVisitor visitor{plan, sinkPipeline};
        for (auto& child : sinkPipeline->sinkOperator->releaseChildren()) {
            auto childVariant = convertOperator(std::move(child));
            std::visit(visitor, childVariant);
        }
    }
    std::cout << "Constructed pipeline plan with " << plan.pipelines.size() << " root pipelines.\n";
    return plan;
}

}
