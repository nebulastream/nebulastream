#include <QueryCompiler/QueryExecutionPlan.hpp>

#include <assert.h>
#include <iostream>
#include <Util/Logger.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace NES {

QueryExecutionPlan::QueryExecutionPlan() : sources(), stages() {
  std::string qId = this->generateAndAssignQueryId();
  NES_DEBUG("QueryExecutionPlan: QEP created with QueryId " << qId)
}

QueryExecutionPlan::QueryExecutionPlan(const std::string& _queryId) : queryId(_queryId), sources(), stages() {}

QueryExecutionPlan::QueryExecutionPlan(const std::string& _queryId,
                                       const std::vector<DataSourcePtr>& _sources,
                                       const std::vector<PipelineStagePtr>& _stages,
                                       const std::map<DataSource*, uint32_t>& _source_to_stage,
                                       const std::map<uint32_t, uint32_t>& _stage_to_dest)
    : queryId(_queryId), sources(_sources), stages(_stages), source_to_stage(_source_to_stage), stage_to_dest(_stage_to_dest)
{
}

QueryExecutionPlan::~QueryExecutionPlan()
{
    NES_DEBUG("destroy qep")
    sources.clear();
    stages.clear();
    source_to_stage.clear();
    stage_to_dest.clear();
}

void QueryExecutionPlan::print() {
   for (auto source : sources) {
     NES_INFO("Source:" << source)
     NES_INFO(
         "\t Generated Buffers=" << source->getNumberOfGeneratedBuffers())
     NES_INFO("\t Generated Tuples=" << source->getNumberOfGeneratedTuples())
     NES_INFO("\t Schema=" << source->getSourceSchemaAsString())
   }
   for (auto window : windows) {
     NES_INFO("WindowHandler:" << window)
     NES_INFO("WindowHandler Result:")
   }
   for (auto sink : sinks) {
     NES_INFO("Sink:" << sink)
     NES_INFO("\t Generated Buffers=" << sink->getNumberOfSentBuffers())
     NES_INFO("\t Generated Tuples=" << sink->getNumberOfSentTuples())
   }
 }


bool QueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, const NES::TupleBufferPtr buf) {
  return false;
}

uint32_t QueryExecutionPlan::stageIdFromSource(DataSource* source) { return source_to_stage[source]; };

const std::vector<DataSourcePtr> QueryExecutionPlan::getSources() const { return sources; }

const std::vector<WindowPtr> QueryExecutionPlan::getWindows() const { return windows; }

const std::vector<DataSinkPtr> QueryExecutionPlan::getSinks() const { return sinks; }

const std::string &QueryExecutionPlan::getQueryId() const {
  return this->queryId;
}

std::string QueryExecutionPlan::generateAndAssignQueryId() {
  boost::uuids::basic_random_generator<boost::mt19937> gen;
  boost::uuids::uuid u = gen();
  this->queryId = boost::uuids::to_string(u);
  return this->queryId;
}

} // namespace NES
BOOST_CLASS_EXPORT(NES::QueryExecutionPlan);
