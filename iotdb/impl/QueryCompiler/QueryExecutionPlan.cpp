#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <assert.h>
#include <iostream>
#include <Util/Logger.hpp>

namespace NES {

QueryExecutionPlan::QueryExecutionPlan() : sources(), stages() {
}

QueryExecutionPlan::QueryExecutionPlan(const std::vector<DataSourcePtr>& _sources,
                                       const std::vector<PipelineStagePtr>& _stages,
                                       const std::map<DataSource*, uint32_t>& _source_to_stage,
                                       const std::map<uint32_t, uint32_t>& _stage_to_dest)
    : sources(_sources), stages(_stages), sourceToStage(_source_to_stage), stageToDest(_stage_to_dest)
{
}

QueryExecutionPlan::~QueryExecutionPlan()
{
    NES_DEBUG("destroy qep")
    sources.clear();
    stages.clear();
    sourceToStage.clear();
    stageToDest.clear();
}

void QueryExecutionPlan::stop(){
  for(auto &stage: stages){
    stage->stop();
  }
}

void QueryExecutionPlan::setup(){
  for(auto &stage: stages){
    stage->setup();
  }
}

void QueryExecutionPlan::start() {
  for(auto &stage: stages){
    stage->start();
  }
}

void QueryExecutionPlan::print() {
   for (auto source : sources) {
     NES_INFO("Source:" << source)
     NES_INFO(
         "\t Generated Buffers=" << source->getNumberOfGeneratedBuffers())
     NES_INFO("\t Generated Tuples=" << source->getNumberOfGeneratedTuples())
     NES_INFO("\t Schema=" << source->getSourceSchemaAsString())
   }
   /*
   for (auto window : windows) {
     NES_INFO("WindowHandler:" << window)
     NES_INFO("WindowHandler Result:")
   }*/
   for (auto sink : sinks) {
     NES_INFO("Sink:" << sink)
     NES_INFO("\t Generated Buffers=" << sink->getNumberOfSentBuffers())
     NES_INFO("\t Generated Tuples=" << sink->getNumberOfSentTuples())
   }
 }


bool QueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, const NES::TupleBufferPtr buf) {
  return false;
}

uint32_t QueryExecutionPlan::stageIdFromSource(DataSource* source) { return sourceToStage[source]; };

const std::vector<DataSourcePtr> QueryExecutionPlan::getSources() const { return sources; }

//const std::vector<WindowPtr> QueryExecutionPlan::getWindows() const { return windows; }

const std::vector<DataSinkPtr> QueryExecutionPlan::getSinks() const { return sinks; }

} // namespace NES
