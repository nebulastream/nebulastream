/*
 * QueryExecutionPlan.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_QUERYEXECUTIONPLAN_H_
#define INCLUDE_QUERYEXECUTIONPLAN_H_
#include <vector>
#include <core/PipelineStage.hpp>
#include <Runtime/DataSource.hpp>

namespace iotdb{


class QueryExecutionPlan {
public:
  virtual bool executeStage(uint32_t pipeline_stage_id, const TupleBuffer &buf);
  const std::vector<DataSourcePtr> getSources() const;
  virtual ~QueryExecutionPlan();
protected:
  QueryExecutionPlan();
  QueryExecutionPlan(const std::vector<DataSourcePtr> &_sources, const std::vector<PipelineStagePtr> &_stages);

  std::vector<DataSourcePtr> sources;
  std::vector<PipelineStagePtr> stages;
};
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

}

#endif /* INCLUDE_QUERYEXECUTIONPLAN_H_ */
