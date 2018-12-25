/*
 * HandCodedQueryExecutionPlan.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_HANDCODEDQUERYEXECUTIONPLAN_H_
#define INCLUDE_HANDCODEDQUERYEXECUTIONPLAN_H_

#include <core/QueryExecutionPlan.hpp>
#include <stdint.h>
class TupleBuffer;

namespace iotdb {

class HandCodedQueryExecutionPlan : public QueryExecutionPlan {
public:
  HandCodedQueryExecutionPlan();
  virtual ~HandCodedQueryExecutionPlan();
  virtual bool executeStage(uint32_t pipeline_stage_id, const TupleBuffer &buf) = 0;
};
}

#endif /* INCLUDE_HANDCODEDQUERYEXECUTIONPLAN_H_ */
