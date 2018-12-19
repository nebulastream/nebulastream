/*
 * PipelineStage.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_PIPELINESTAGE_H_
#define INCLUDE_PIPELINESTAGE_H_

#include "../core/TupleBuffer.h"
class PipelineStage {
public:
  /** \brief process input tuple buffer */
  bool execute(TupleBuffer buf);
};

#endif /* INCLUDE_PIPELINESTAGE_H_ */
