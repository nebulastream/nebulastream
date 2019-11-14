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

namespace codegen {
struct TupleBuffer {
  void *data;
  uint64_t buffer_size;
  uint64_t tuple_size_bytes;
  uint64_t num_tuples;
};
struct WindowState {
  void *window_state;
};
struct __attribute__((packed)) InputTuple {
  uint32_t id;
  uint64_t value;
};
struct __attribute__((packed)) ResultTuple {
  uint32_t id;
  uint64_t value;
};
}

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
    //WindowState *state = nullptr;

    bool ret = ptr->get()->execute(input_buffers, nullptr, nullptr, outputBuffer.get());

    for (const DataSinkPtr &s: this->getSinks()) {
      s->writeData(outputBuffer);
    }
    return ret;
  };
 protected:
  InputQuery *query;
  PipelineStagePtr *ptr;
 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & boost::serialization::base_object<QueryExecutionPlan>(*this);
  }
};

typedef std::shared_ptr<GeneratedQueryExecutionPlan> GeneratedQueryExecutionPlanPtr;

}
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::GeneratedQueryExecutionPlan)
#endif /* INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_ */
