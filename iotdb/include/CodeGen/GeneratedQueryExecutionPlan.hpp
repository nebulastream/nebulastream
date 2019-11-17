/*
 * HandCodedQueryExecutionPlan.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_
#define INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_

#include <CodeGen/QueryExecutionPlan.hpp>
#include <CodeGen/PipelineStage.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <stdint.h>
#include <API/InputQuery.hpp>
// class TupleBuffer;

namespace iotdb {

class GeneratedQueryExecutionPlan : public QueryExecutionPlan {
 public:
  GeneratedQueryExecutionPlan();
  GeneratedQueryExecutionPlan(InputQuery *query, PipelineStagePtr *ptr);

  bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf) override {
    std::vector<TupleBuffer *> input_buffers(1, buf.get());

    TupleBufferPtr outputBuffer = BufferManager::instance().getBuffer();
    outputBuffer->setTupleSizeInBytes(buf->getTupleSizeInBytes());

    // TODO: add support for window operators here
    WindowSliceStore *state = nullptr;
    WindowManager *manager = nullptr;

    bool ret = pipeline_stage_ptr_->get()->execute(input_buffers, state, manager, outputBuffer.get());

    for (const DataSinkPtr &s: this->getSinks()) {
      s->writeData(outputBuffer);
    }
    return ret;
  };
 protected:
  InputQuery *query;
  PipelineStagePtr *pipeline_stage_ptr_;
};

typedef std::shared_ptr<GeneratedQueryExecutionPlan> GeneratedQueryExecutionPlanPtr;

}
#endif /* INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_ */
