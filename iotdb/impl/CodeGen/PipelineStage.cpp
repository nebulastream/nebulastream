/*
 * PipelineStage.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <CodeGen/PipelineStage.hpp>
#include <Util/ErrorHandling.hpp>
#include <iostream>
#include "../../include/SourceSink/DataSink.hpp"

namespace iotdb {

bool PipelineStage::execute(const std::vector<TupleBuffer *> &input_buffers,
                            WindowSliceStore *state,
                            WindowManager *window_manager,
                            TupleBuffer *result_buf) {
  std::cout << "Execute a Pipeline Stage!" << std::endl;
  uint32_t ret = execute_impl(input_buffers, state, window_manager, result_buf);
  if (ret) IOTDB_FATAL_ERROR("Execution of Compled PipelineStage Failed!");

  return true;
}

PipelineStage::~PipelineStage() {}

class CPipelineStage : public PipelineStage {
 public:
  CPipelineStage(CompiledCCodePtr compiled_code);
  CPipelineStage(const CPipelineStage &);

  const PipelineStagePtr copy() const override;

 protected:
  uint32_t execute_impl(const std::vector<TupleBuffer *> &input_buffers,
                        WindowSliceStore *state,
                        WindowManager *window_manager,
                        TupleBuffer *result_buf) final;
  virtual uint32_t callCFunction(TupleBuffer **tuple_buffers,
                                 WindowSliceStore *state,
                                 WindowManager *window_manager,
                                 TupleBuffer *result_buffer);

  CompiledCCodePtr compiled_code_;
};

CPipelineStage::CPipelineStage(CompiledCCodePtr compiled_code) : compiled_code_(compiled_code) {}

CPipelineStage::CPipelineStage(const CPipelineStage &other) {
  /* consider deep copying this! */
  this->compiled_code_ = other.compiled_code_;
}

const PipelineStagePtr CPipelineStage::copy() const { return PipelineStagePtr(new CPipelineStage(*this)); }

uint32_t CPipelineStage::execute_impl(const std::vector<TupleBuffer *> &input_buffers,
                                      WindowSliceStore *state,
                                      WindowManager *window_manager,
                                      TupleBuffer *result_buf) {
  return callCFunction((TupleBuffer **) input_buffers.data(), state, window_manager, result_buf);
}

typedef uint32_t (*SharedCLibPipelineQueryPtr)(TupleBuffer **, WindowSliceStore *, WindowManager *, TupleBuffer *);

uint32_t CPipelineStage::callCFunction(TupleBuffer **tuple_buffers,
                                       WindowSliceStore *state,
                                       WindowManager *window_manager,
                                       TupleBuffer *result_buffer) {
  return (*compiled_code_->getFunctionPointer<SharedCLibPipelineQueryPtr>(
      "_Z14compiled_queryPP11TupleBufferPN5iotdb16WindowSliceStoreIlEEPNS2_13WindowManagerES0_"))(tuple_buffers,
                                                                                                  state, window_manager,
                                                                                                  result_buffer);
}

PipelineStagePtr createPipelineStage(const CompiledCCodePtr compiled_code) {
  return PipelineStagePtr(new CPipelineStage(compiled_code));
}

class DataSinkPiplineStage : public PipelineStage {
 public:
  DataSinkPiplineStage(DataSinkPtr sink);

 protected:
  DataSinkPtr sink;
  uint32_t execute_impl(const std::vector<TupleBuffer *> &input_buffers,
                        WindowSliceStore *state,
                        WindowManager *window_manager,
                        TupleBuffer *result_buf) final;
};

DataSinkPiplineStage::DataSinkPiplineStage(DataSinkPtr sink) : sink(sink) {}

uint32_t DataSinkPiplineStage::execute_impl(const std::vector<TupleBuffer *> &input_buffers,
                                            WindowSliceStore *state,
                                            WindowManager *window_manager,
                                            TupleBuffer *result_buf) {
  for (auto &buf : input_buffers) {
    TupleBufferPtr buffer_ptr(buf);
    if (!sink->writeData(buffer_ptr))
      return 1;
    buffer_ptr.reset();
  }
  return 0;
  //    if(sink->writeData(input_buffers))
  //        return 0;
  //    else
  //        return 1;
}
} // namespace iotdb
