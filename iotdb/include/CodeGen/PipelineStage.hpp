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

namespace iotdb {

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class PipelineStage {
public:
  /** \brief process input tuple buffer */
  bool execute(TupleBuffer buf);
  virtual const PipelineStagePtr copy() const = 0;
  virtual ~PipelineStage();
protected:
  virtual TupleBuffer execute_impl() = 0;
};
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;
}

#endif /* INCLUDE_PIPELINESTAGE_H_ */
