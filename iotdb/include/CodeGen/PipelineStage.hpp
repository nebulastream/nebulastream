/*
 * PipelineStage.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_PIPELINESTAGE_H_
#define INCLUDE_PIPELINESTAGE_H_
#include <Core/TupleBuffer.hpp>
#include <memory>
#include <vector>

namespace iotdb {

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class WindowState;

class PipelineStage {
public:
  /** \brief process input tuple buffer */
  bool execute(TupleBuffer buf, WindowState* state);
  virtual const PipelineStagePtr copy() const = 0;
  virtual ~PipelineStage();
protected:
  virtual TupleBuffer execute_impl(std::vector<TupleBuffer*>& input_buffers) = 0;
};
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class CompiledCCode;
typedef std::shared_ptr<CompiledCCode> CompiledCCodePtr;

PipelineStagePtr createPipelineStage(const CompiledCCodePtr compiled_code);

}

#endif /* INCLUDE_PIPELINESTAGE_H_ */
