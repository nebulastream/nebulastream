
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <CodeGen/GeneratedQueryExecutionPlan.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::GeneratedQueryExecutionPlan);

namespace iotdb {

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan() : pipeline_stage_ptr_() {

}
GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan(PipelineStagePtr ptr)
    : pipeline_stage_ptr_(ptr) {
}

bool GeneratedQueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, const iotdb::TupleBufferPtr buf) {
  std::vector<TupleBuffer *> input_buffers(1, buf.get());

  TupleBufferPtr outputBuffer = BufferManager::instance().getBuffer();
  outputBuffer->setTupleSizeInBytes(buf->getTupleSizeInBytes());
  bool ret;
  // if the pipeline contains a window.
  if (!this->windows.empty()) {
    auto window = this->windows[pipeline_stage_id];
    void *state = window->getWindowState();
    auto window_manager = window->getWindowManager();
    pipeline_stage_ptr_->execute(input_buffers, state, window_manager.get(), outputBuffer.get());
  }else {
     ret = pipeline_stage_ptr_->execute(input_buffers, nullptr, nullptr, outputBuffer.get());
  }
  // only write data to the sink if the pipeline produced some output
  if (outputBuffer->getNumberOfTuples() > 0) {
    for (const DataSinkPtr &s: this->getSinks()) {
      s->writeData(outputBuffer);
    }
  }
  return ret;
}

} // namespace iotdb
