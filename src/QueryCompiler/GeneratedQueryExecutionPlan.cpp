#include <NodeEngine/QueryManager.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <Util/Logger.hpp>
#include <utility>
namespace NES {

GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan(
    QueryId queryId,
    QueryExecutionPlanId queryExecutionPlanId,
    std::vector<DataSourcePtr>&& sources,
    std::vector<DataSinkPtr>&& sinks,
    std::vector<PipelineStagePtr>&& stages,
    QueryManagerPtr&& queryManager,
    BufferManagerPtr&& bufferManager)
    : QueryExecutionPlan(queryId, queryExecutionPlanId, std::move(sources), std::move(sinks), std::move(stages), std::move(queryManager), std::move(bufferManager)) {
    // sanity checks
    for (auto& stage : this->stages) {
        NES_ASSERT(!!stage, "Invalid stage");
    }
}

}// namespace NES
