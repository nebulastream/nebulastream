#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <Util/Logger.hpp>
namespace NES {

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan() : QueryExecutionPlan() {}

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan(const std::string& queryId) : QueryExecutionPlan() {
}

bool GeneratedQueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, TupleBuffer& inputBuffer) {
    //TODO this should be changed such that we provide the outputbuffer too
    auto outputBuffer = this->buffMgnr->getBufferBlocking();
    outputBuffer.setTupleSizeInBytes(inputBuffer.getTupleSizeInBytes());
    NES_DEBUG("inputBuffer->getTupleSizeInBytes()=" << inputBuffer.getTupleSizeInBytes());
    bool ret = stages[pipeline_stage_id]->execute(inputBuffer, outputBuffer);
    // only write data to the sink if the pipeline produced some output
    if (outputBuffer.getNumberOfTuples() > 0) {
        if (stages.size() <= pipeline_stage_id) {
            NES_DEBUG("QueryExecutionPlan: send output buffer to next pipeline");
            // todo schedule dispatching as a new task
            this->executeStage(pipeline_stage_id + 1, outputBuffer);
        } else {
            for (const DataSinkPtr& s: this->getSinks()) {
                NES_DEBUG("QueryExecutionPlan: end output buffer to sink");
                s->writeData(outputBuffer);
            }
        }
    }
    return ret;
}

} // namespace NES
