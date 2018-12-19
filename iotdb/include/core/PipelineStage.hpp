/*
 * PipelineStage.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_PIPELINESTAGE_H_
#define INCLUDE_PIPELINESTAGE_H_
#include "TupleBuffer.hpp"
#include <memory>
class PipelineStage {
public:
  /** \brief process input tuple buffer */
  bool execute(TupleBuffer buf);
};
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

#endif /* INCLUDE_PIPELINESTAGE_H_ */
