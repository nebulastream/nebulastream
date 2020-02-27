#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>

namespace NES {

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan() : QueryExecutionPlan() {}

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan(const std::string& queryId) : QueryExecutionPlan(){
}

bool GeneratedQueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr inputBuffer) {
  TupleBufferPtr outputBuffer = BufferManager::instance().getBuffer();
  outputBuffer->setTupleSizeInBytes(inputBuffer->getTupleSizeInBytes());
  bool ret = stages[pipeline_stage_id]->execute(inputBuffer, outputBuffer);
  // only write data to the sink if the pipeline produced some output
  if (outputBuffer->getNumberOfTuples() > 0) {
    for (const DataSinkPtr &s: this->getSinks()) {
      s->writeData(outputBuffer);
    }
  }
  return ret;
}

} // namespace NES
