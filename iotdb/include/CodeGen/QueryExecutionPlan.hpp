/*
 * QueryExecutionPlan.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_QUERYEXECUTIONPLAN_H_
#define INCLUDE_QUERYEXECUTIONPLAN_H_
#include <Runtime/DataSource.hpp>
#include <CodeGen/PipelineStage.hpp>
#include <vector>
#include <Runtime/DataSink.hpp>
#include <Runtime/Window.hpp>
#include <map>

namespace iotdb {

class QueryExecutionPlan {
public:
  virtual bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf);
  const std::vector<DataSourcePtr> getSources() const;
  const std::vector<WindowPtr> getWindows() const;
  const std::vector<DataSinkPtr> getSinks() const;

  uint32_t stageIdFromSource(DataSource * source);
  virtual ~QueryExecutionPlan();
  size_t getQueryResult(std::string name){return qResult[name];};

  void addDataSource(DataSourcePtr source)
	{
	   sources.push_back(source);
	}

	 void addDataSink(DataSinkPtr sink)
	{
	   sinks.push_back(sink);
	}
	 void addWindow(WindowPtr window)
	{
	   windows.push_back(window);
	}
protected:
  QueryExecutionPlan();
  QueryExecutionPlan(const std::vector<DataSourcePtr> &_sources, 
          const std::vector<PipelineStagePtr> &_stages, 
          const std::map<DataSource *, uint32_t> &_source_to_stage,
          const std::map<uint32_t, uint32_t> &_stage_to_dest);

  std::vector<DataSourcePtr> sources;
  std::vector<DataSinkPtr> sinks;
  std::vector<WindowPtr> windows;

  std::vector<PipelineStagePtr> stages;
  std::map<DataSource *, uint32_t> source_to_stage;
  std::map<uint32_t, uint32_t> stage_to_dest;
  std::map<std::string, size_t> qResult;

};
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;
}

#endif /* INCLUDE_QUERYEXECUTIONPLAN_H_ */
