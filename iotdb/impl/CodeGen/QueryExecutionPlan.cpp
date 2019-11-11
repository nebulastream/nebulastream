#include <CodeGen/QueryExecutionPlan.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::QueryExecutionPlan);

#include <assert.h>
#include <iostream>

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

uint32_t QueryExecutionPlan::stageIdFromSource(DataSource* source) { return source_to_stage[source]; };

const std::vector<DataSourcePtr> QueryExecutionPlan::getSources() const { return sources; }

const std::vector<WindowPtr> QueryExecutionPlan::getWindows() const { return windows; }

const std::vector<DataSinkPtr> QueryExecutionPlan::getSinks() const { return sinks; }

bool QueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf)
{
    assert(0);
    void * state;
    WindowManager* windowManager;
    TupleBuffer result_buf{nullptr, 0, 0, 0};
    std::vector<TupleBuffer*> v;
    v.push_back((TupleBuffer*)&buf);
    bool ret = stages[pipeline_stage_id]->execute(v, state, windowManager, &result_buf);
    return ret;
}

const QueryExecutionPlanPtr createTestQEP() { return std::make_shared<QueryExecutionPlan>(); }

} // namespace iotdb
