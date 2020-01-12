/*
 * PipelineStage.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_PIPELINESTAGE_H_
#define INCLUDE_PIPELINESTAGE_H_
#include <memory>
#include <vector>
#include "../NodeEngine/TupleBuffer.hpp"

namespace NES {

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

template<class PartialAggregateType>
class WindowSliceStore;

class WindowManager;

class PipelineStage {
 public:
  /** \brief process input tuple buffer */
  bool execute(const std::vector<TupleBuffer *> &input_buffers,
               void *state,
               WindowManager *window_manager,
               TupleBuffer *result_buf);
  virtual const PipelineStagePtr copy() const = 0;
  virtual ~PipelineStage();

 protected:
  virtual uint32_t execute_impl(const std::vector<TupleBuffer *> &input_buffers,
                                void *state,
                                WindowManager *window_manager,
                                TupleBuffer *result_buf) = 0;
};
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class CompiledCCode;
typedef std::shared_ptr<CompiledCCode> CompiledCCodePtr;

PipelineStagePtr createPipelineStage(const CompiledCCodePtr compiled_code);

} // namespace NES

#endif /* INCLUDE_PIPELINESTAGE_H_ */
