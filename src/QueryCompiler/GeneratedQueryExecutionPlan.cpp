#include <NodeEngine/QueryManager.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <Util/Logger.hpp>
namespace NES {

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan() : QueryExecutionPlan("") {}

GeneratedQueryExecutionPlanPtr GeneratedQueryExecutionPlan::create() {
    return std::make_shared<GeneratedQueryExecutionPlan>();
}

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan(const std::string& queryId) : QueryExecutionPlan(queryId) {
}

bool GeneratedQueryExecutionPlan::executeStage(uint32_t pipelineStageId, TupleBuffer& inputBuffer) {
    // check if we should pass this buffer to a sink or to a pipeline
    if (pipelineStageId >= numberOfPipelineStages()) {
        NES_DEBUG("QueryExecutionPlan: output buffer to " << this->getSinks().size() << " sinks");
        for (const auto& s : this->getSinks()) {
            s->writeData(inputBuffer);
        }
        return true;
    }
    NES_DEBUG("QueryExecutionPlan: process buffer with pipeline - " << pipelineStageId);

    // create emit function handler
    auto emitFunctionHandler = [this, pipelineStageId](TupleBuffer& buffer) {
        NES_DEBUG("QueryExecutionPlan: received buffer from pipelinestage:" << pipelineStageId << " with "
                                                                            << buffer.getNumberOfTuples() << " tuples.");
        // ignore the buffer if it is empty
        if (buffer.getNumberOfTuples() > 0) {
            // send the buffer to the next pipeline stage
            auto nextStage = this->stages.at(pipelineStageId)->getNextStage();
            auto nextStageId = nextStage == nullptr ? pipelineStageId + 1 : nextStage->getPipeStageId();
            executeStage(nextStageId, buffer);//this->stages.at(pipelineStageId).nextStage()
        }
    };
    // create execution context
    auto queryExecutionContext = PipelineExecutionContext(bufferManager, emitFunctionHandler);

    NES_DEBUG("QueryExecutionPlan: execute pipeline stage " << pipelineStageId);
    stages[pipelineStageId]->execute(inputBuffer, queryExecutionContext);

    return true;
}

}// namespace NES
