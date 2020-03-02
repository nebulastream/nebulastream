#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <Util/Logger.hpp>
namespace NES {

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan() : QueryExecutionPlan() {}

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan(const std::string& queryId) : QueryExecutionPlan() {
}

bool GeneratedQueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr inputBuffer) {
    TupleBufferPtr outputBuffer = BufferManager::instance().getBuffer();
    outputBuffer->setTupleSizeInBytes(inputBuffer->getTupleSizeInBytes());
    bool ret = stages[pipeline_stage_id]->execute(inputBuffer, outputBuffer);
    // only write data to the sink if the pipeline produced some output
    if (outputBuffer->getNumberOfTuples() > 0) {
        if (stages.size() <= pipeline_stage_id) {
            this->executeStage(pipeline_stage_id + 1, outputBuffer);
        } else {
            for (const DataSinkPtr& s: this->getSinks()) {
                NES_DEBUG("Qep: send to sink");
                s->writeData(outputBuffer);
            }
        }
    }
    return ret;
}

} // namespace NES
