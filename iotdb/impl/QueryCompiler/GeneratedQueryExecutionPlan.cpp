#include <NodeEngine/QueryManager.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <Util/Logger.hpp>
namespace NES {

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan() : QueryExecutionPlan("") {}

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan(const std::string& queryId) : QueryExecutionPlan(queryId) {
}

bool GeneratedQueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, TupleBuffer& inputBuffer) {
    if (stages.size() <= pipeline_stage_id) {
        for (const DataSinkPtr& s: this->getSinks()) {
            NES_DEBUG("QueryExecutionPlan: end output buffer to sink");
            s->writeData(inputBuffer);
        }
        return 0;
    }
    std::function<void(TupleBuffer&)>
        callback = [this, pipeline_stage_id](TupleBuffer& buffer) { executeStage(pipeline_stage_id + 1, buffer); };
    auto queryExecutionContext = PipelineExecutionContext(bufferManager, callback);
    bool ret = stages[pipeline_stage_id]->execute(inputBuffer, queryExecutionContext);

    //TODO this should be changed such that we provide the outputbuffer too
    //NES_DEBUG("GeneratedQueryExecutionPlan::executeStage get buffer")
    //auto outputBuffer = bufferManager->getBufferBlocking();
    //NES_DEBUG("GeneratedQueryExecutionPlan::executeStage got buffer of size=" << outputBuffer.getBufferSize())
    //outputBuffer.setTupleSizeInBytes(inputBuffer.getTupleSizeInBytes());
    //NES_DEBUG("inputBuffer->getTupleSizeInBytes()=" << inputBuffer.getTupleSizeInBytes());


    /*// only write data to the sink if the pipeline produced some output
    if (outputBuffer.getNumberOfTuples() > 0) {
        if (stages.size() <= pipeline_stage_id) {
            NES_DEBUG("QueryExecutionPlan: send output buffer to next pipeline");
            // todo schedule dispatching as a new task
            executeStage(pipeline_stage_id + 1, outputBuffer);
        } else {

        }
    }*/


    return 0;
}

}// namespace NES
