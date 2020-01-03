#include <CodeGen/QueryExecutionPlan.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::QueryExecutionPlan);

#include <assert.h>
#include <iostream>
#include <Util/Logger.hpp>

namespace iotdb {

QueryExecutionPlan::QueryExecutionPlan() : sources(), stages() {}

QueryExecutionPlan::QueryExecutionPlan(const std::vector<DataSourcePtr>& _sources,
                                       const std::vector<PipelineStagePtr>& _stages,
                                       const std::map<DataSource*, uint32_t>& _source_to_stage,
                                       const std::map<uint32_t, uint32_t>& _stage_to_dest)
    : sources(_sources), stages(_stages), source_to_stage(_source_to_stage), stage_to_dest(_stage_to_dest)
{
}

QueryExecutionPlan::~QueryExecutionPlan()
{
    IOTDB_DEBUG("destroy qep")
    sources.clear();
    stages.clear();
    source_to_stage.clear();
    stage_to_dest.clear();
}

void QueryExecutionPlan::print() {
   for (auto source : sources) {
     IOTDB_INFO("Source:" << source)
     IOTDB_INFO(
         "\t Generated Buffers=" << source->getNumberOfGeneratedBuffers())
     IOTDB_INFO("\t Generated Tuples=" << source->getNumberOfGeneratedTuples())
     IOTDB_INFO("\t Schema=" << source->getSourceSchemaAsString())
   }
   for (auto window : windows) {
     IOTDB_INFO("WindowHandler:" << window)
     IOTDB_INFO("WindowHandler Result:")
   }
   for (auto sink : sinks) {
     IOTDB_INFO("Sink:" << sink)
     IOTDB_INFO("\t Generated Buffers=" << sink->getNumberOfSentBuffers())
     IOTDB_INFO("\t Generated Tuples=" << sink->getNumberOfSentTuples())
   }
 }


bool QueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, const iotdb::TupleBufferPtr buf) {
  return false;
}

uint32_t QueryExecutionPlan::stageIdFromSource(DataSource* source) { return source_to_stage[source]; };

const std::vector<DataSourcePtr> QueryExecutionPlan::getSources() const { return sources; }

const std::vector<WindowPtr> QueryExecutionPlan::getWindows() const { return windows; }

const std::vector<DataSinkPtr> QueryExecutionPlan::getSinks() const { return sinks; }

} // namespace iotdb
