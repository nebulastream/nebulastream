#ifndef INCLUDE_QUERYEXECUTIONPLAN_H_
#define INCLUDE_QUERYEXECUTIONPLAN_H_
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include <map>
#include <QueryCompiler/PipelineStage.hpp>
#include <Windows/WindowHandler.hpp>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/DataSource.hpp>

namespace NES {
class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class QueryExecutionPlan {
 public:
  QueryExecutionPlan();
  virtual ~QueryExecutionPlan();

  void setup();
  void start();
  void stop();

  virtual bool executeStage(uint32_t pipeline_stage_id,
                            const TupleBufferPtr buf);
  const std::vector<DataSourcePtr> getSources() const;
  const std::vector<DataSinkPtr> getSinks() const;

  uint32_t stageIdFromSource(DataSource* source);

  size_t getQueryResult(std::string name) {
    return qResult[name];
  };

  void addDataSource(DataSourcePtr source) {
    sources.push_back(source);
  }

  void addDataSink(DataSinkPtr sink) {
    sinks.push_back(sink);
  }

  void addPipelineStage(PipelineStagePtr pipelineStage){
    stages.push_back(pipelineStage);
  }

  uint32_t numberOfPipelineStages(){
    return stages.size();
  }

  void print();

 protected:
  QueryExecutionPlan(const std::vector<DataSourcePtr> &_sources,
                     const std::vector<PipelineStagePtr> &_stages,
                     const std::map<DataSource *, uint32_t> &_source_to_stage,
                     const std::map<uint32_t, uint32_t> &_stage_to_dest);

  std::vector<DataSourcePtr> sources;
  std::vector<DataSinkPtr> sinks;
  std::vector<PipelineStagePtr> stages;
  std::map<DataSource*, uint32_t> sourceToStage;
  std::map<uint32_t, uint32_t> stageToDest;
  std::map<std::string, size_t> qResult;
};
const QueryExecutionPlanPtr createTestQEP();

}  // namespace NES

#endif /* INCLUDE_QUERYEXECUTIONPLAN_H_ */
