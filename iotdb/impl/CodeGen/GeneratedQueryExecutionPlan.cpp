/*
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <CodeGen/GeneratedQueryExecutionPlan.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::GeneratedQueryExecutionPlan);

#include <CodeGen/QueryExecutionPlan.hpp>
#include <API/InputQuery.hpp>

namespace iotdb {
    GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan():query(), pipeline_stage_ptr_() {

    }
    GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan(InputQuery* query, PipelineStagePtr ptr)
            : query(query), pipeline_stage_ptr_(ptr) {
    }

    bool GeneratedQueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, const iotdb::TupleBufferPtr buf) {
      std::vector<TupleBuffer *> input_buffers(1, buf.get());

      TupleBufferPtr outputBuffer = BufferManager::instance().getBuffer();
      outputBuffer->setTupleSizeInBytes(buf->getTupleSizeInBytes());

      auto window = this->windows[pipeline_stage_id];
      void * state = window->getWindowState();
      auto windowManager = window->getWindowManager();

      bool ret = pipeline_stage_ptr_->execute(input_buffers, state, windowManager.get(), outputBuffer.get());

      for (const DataSinkPtr &s: this->getSinks()) {
        s->writeData(outputBuffer);
      }
      return ret;
    }

} // namespace iotdb
