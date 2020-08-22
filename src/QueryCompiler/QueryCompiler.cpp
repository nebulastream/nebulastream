#include <NodeEngine/QueryManager.hpp>
#include <set>
#include <QueryCompiler/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlanBuilder.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <utility>
namespace NES {

QueryCompiler::QueryCompiler() {
    // nop
}

QueryCompilerPtr QueryCompiler::create() {
    return std::make_shared<QueryCompiler>();
}

// TODO compiler folks please check the following statements
/**
 * The following code builds a dataflow graph of pipeline stages. It does so by following these steps:
 * 1. generates executable pipelines for an input query plan that is represented as a tree (should be graph) whose root (should be sink nodes) is a sink operator
 * 2. performs a BFS visit of the tree to figure out producer-consumer relations among pipeline stages (this is necessary because we need to label operators)
 * 3. while BFS-visiting the tree, it inserts the pipelines in a sorted map (leaves stages are the first)
 * 4. it scans the map to build pipeline stages. This way, we know the consumer set for each pipeline stage (or its sinks) and we can generate buffer emitters
 */

void QueryCompiler::compile(GeneratedQueryExecutionPlanBuilder& qepBuilder, OperatorNodePtr queryPlan) {
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();
    queryPlan->as<GeneratableOperator>()->produce(codeGenerator, context, std::cout);
    compilePipelineStages(qepBuilder, codeGenerator, context);
}

namespace detail {

class PipelineStageHolder {
  public:
    uint32_t currentStageId;
    ExecutablePipelinePtr executablePipeline;
    WindowHandlerPtr windowHandler;
    std::set<uint32_t> producers;
    std::set<uint32_t> consumers;

  public:
    PipelineStageHolder() = default;

    PipelineStageHolder(uint32_t currentStageId, ExecutablePipelinePtr executablePipeline, WindowHandlerPtr windowHandler = WindowHandlerPtr())
        : currentStageId(currentStageId), executablePipeline(std::move(executablePipeline)), windowHandler(std::move(windowHandler)) {
        // nop
    }
};

void generateExecutablePipelines(
    CodeGeneratorPtr codeGenerator,
    BufferManagerPtr bufferManager,
    QueryManagerPtr queryManagerPtr,
    PipelineContextPtr context,
    std::map<uint32_t, PipelineStageHolder, std::greater<>>& accumulator) {
    // BFS visit to figure out producer-consumer relations among pipelines
    std::deque<std::tuple<int32_t, int32_t, PipelineContextPtr>> queue;
    queue.emplace_back(0, -1, std::move(context));
    while (!queue.empty()) {
        auto [currIndex, consumerIndex, currContext] = queue.front();
        queue.pop_front();
        try {
            auto executablePipeline = codeGenerator->compile(currContext->code);
            if (executablePipeline == nullptr) {
                NES_ERROR("Cannot compile pipeline " << currContext->code);
                NES_THROW_RUNTIME_ERROR("Cannot compile pipeline");
            }
            if (currContext->hasWindow()) {
                auto windowHandler = createWindowHandler(
                    currContext->getWindow(),
                    queryManagerPtr,
                    bufferManager);
                accumulator[currIndex] = PipelineStageHolder(currIndex, executablePipeline, windowHandler);
            } else {
                accumulator[currIndex] = PipelineStageHolder(currIndex, executablePipeline, nullptr);
            }
            if (consumerIndex >= 0) {
                accumulator[currIndex].consumers.emplace(consumerIndex);
            }
        } catch (std::exception& err) {
            NES_ERROR("Error while compiling pipeline: " << err.what());
            NES_THROW_RUNTIME_ERROR("Cannot compile pipeline");
        }
        uint32_t i = 1;
        for (const auto& nextPipelineContext : currContext->getNextPipelineContexts()) {
            queue.emplace_back(currIndex + i, currIndex, nextPipelineContext);
            accumulator[currIndex].producers.emplace(currIndex + i);
            i++;
        }
    }
}
}// namespace detail
void QueryCompiler::compilePipelineStages(
    GeneratedQueryExecutionPlanBuilder& builder,
    CodeGeneratorPtr codeGenerator,
    PipelineContextPtr context) {

    std::map<uint32_t, detail::PipelineStageHolder, std::greater<>> executableStages;
    detail::generateExecutablePipelines(std::move(codeGenerator), builder.getBufferManager(), builder.getQueryManager(), std::move(context), executableStages);

    if (executableStages.empty()) {
        NES_ERROR("compilePipelineStages failure: no pipelines to generate");
        NES_THROW_RUNTIME_ERROR("No pipelines generated");
    }

    std::map<uint32_t, PipelineStagePtr> pipelines;
    for (auto it = executableStages.rbegin(), last = executableStages.rend(); it != last; ++it) {
        auto& [stageId, holder] = *it;
        QueryExecutionContextPtr executionContext;
        if (!holder.consumers.empty()) {
            // invoke next pipeline
            std::vector<PipelineStagePtr> childPipelines;
            for (auto childStageId : holder.consumers) {
                childPipelines.emplace_back(pipelines[childStageId]);
            }
            executionContext = std::make_shared<PipelineExecutionContext>(
                builder.getBufferManager(),
                [childPipelines](TupleBuffer& buffer) {
                    for (auto& childPipeline : childPipelines) {
                        childPipeline->execute(buffer);
                    }
                });
        } else {
            // invoke sink
            auto& sinks = builder.getSinks();
            if (sinks.empty()) {
                NES_ERROR("compilePipelineStages failure: no sinks for " << builder.getQueryId());
                NES_THROW_RUNTIME_ERROR("No sinks available in query plan");
            }
            executionContext = std::make_shared<PipelineExecutionContext>(
                builder.getBufferManager(),
                [sinks](TupleBuffer& buffer) {
                    for (auto& sink : sinks) {
                        sink->writeData(buffer);
                    }
                });
        }
        PipelineStagePtr pipelineStage = PipelineStage::create(
            stageId,
            builder.getQueryId(),
            holder.executablePipeline,
            executionContext,
            pipelines[*holder.consumers.begin()],
            holder.windowHandler);

        builder.addPipelineStage(pipelineStage);
        pipelines[stageId] = pipelineStage;
    }
}

QueryCompilerPtr createDefaultQueryCompiler() {
    return QueryCompiler::create();
}

}// namespace NES