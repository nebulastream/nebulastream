/*
 * QueryExecutionPlan.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_QUERYEXECUTIONPLAN_H_
#define INCLUDE_QUERYEXECUTIONPLAN_H_

class QueryExecutionPlan {
public:
  virtual bool executeStage(uint32_t pipeline_stage_id, const TupleBuffer &buf);
  const std::vector<DataSourcePtr> getSources() const;

protected:
  QueryExecutionPlan();
  QueryExecutionPlan(const std::vector<DataSourcePtr> &_sources, const std::vector<PipelineStagePtr> &_stages);

  std::vector<DataSourcePtr> sources;
  std::vector<PipelineStagePtr> stages;
};

#endif /* INCLUDE_QUERYEXECUTIONPLAN_H_ */
