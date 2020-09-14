#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlanBuilder.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Util/Logger.hpp>
#include <utility>
namespace NES {

GeneratedQueryExecutionPlanBuilder::GeneratedQueryExecutionPlanBuilder() {
    setBufferManager(nullptr);
    setQueryManager(nullptr);
    setCompiler(nullptr);
    setQueryId(0);
}

GeneratedQueryExecutionPlanBuilder GeneratedQueryExecutionPlanBuilder::create() {
    return GeneratedQueryExecutionPlanBuilder();
}

BufferManagerPtr GeneratedQueryExecutionPlanBuilder::getBufferManager() const {
    return bufferManager;
}

QuerySubPlanId GeneratedQueryExecutionPlanBuilder::getQueryId() const {
    return queryId;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::addPipelineStage(PipelineStagePtr pipelineStagePtr) {
    stages.push_back(pipelineStagePtr);
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::setBufferManager(BufferManagerPtr bufferManager) {
    this->bufferManager = std::move(bufferManager);
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::setQueryManager(QueryManagerPtr queryManager) {
    this->queryManager = std::move(queryManager);
    return *this;
}

QueryManagerPtr GeneratedQueryExecutionPlanBuilder::getQueryManager() const {
    return queryManager;
}

QueryExecutionPlanPtr GeneratedQueryExecutionPlanBuilder::build() {
    NES_ASSERT(bufferManager, "GeneratedQueryExecutionPlanBuilder: Invalid bufferManager");
    NES_ASSERT(queryManager, "GeneratedQueryExecutionPlanBuilder: Invalid queryManager");
    NES_ASSERT(!sources.empty(), "GeneratedQueryExecutionPlanBuilder: Invalid number of sources");
    NES_ASSERT(!sinks.empty(), "GeneratedQueryExecutionPlanBuilder: Invalid number of sinks");
    NES_ASSERT(queryId != INVALID_QUERY_ID, "GeneratedQueryExecutionPlanBuilder: Invalid Query Id");
    NES_ASSERT(querySubPlanId != INVALID_QUERY_SUB_PLAN_ID, "GeneratedQueryExecutionPlanBuilder: Invalid Query Subplan Id");
    NES_ASSERT(queryCompiler, "GeneratedQueryExecutionPlanBuilder: Invalid compiler or no stages");

    if (stages.empty() && !leaves.empty()) {
        for (auto& operatorPlan : leaves) {
            queryCompiler->compile(*this, operatorPlan);
        }
        NES_ASSERT(!stages.empty(), "GeneratedQueryExecutionPlanBuilder: No stages after query compilation");
        std::reverse(stages.begin(), stages.end());// this is necessary, check plan generator documentation
    }
    for(auto& stage : stages)
    {
        if(stage->hasWindowHandler())
        {
            NES_ASSERT(winDef, "GeneratedQueryExecutionPlanBuilder: windowDef");
            NES_ASSERT(schema, "GeneratedQueryExecutionPlanBuilder: schema");
            stage->setWinDef(winDef);
            stage->setSchema(schema);
        }

    }
    return std::make_shared<GeneratedQueryExecutionPlan>(queryId, querySubPlanId, std::move(sources), std::move(sinks), std::move(stages), std::move(queryManager), std::move(bufferManager));
}

std::vector<DataSinkPtr>& GeneratedQueryExecutionPlanBuilder::getSinks() {
    return sinks;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::addSink(DataSinkPtr sink) {
    sinks.emplace_back(sink);
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::addSource(DataSourcePtr source) {
    sources.emplace_back(source);
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::setQueryId(QueryId queryId) {
    this->queryId = std::move(queryId);
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::setCompiler(QueryCompilerPtr queryCompiler) {
    this->queryCompiler = std::move(queryCompiler);
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::addOperatorQueryPlan(OperatorNodePtr queryPlan) {
    leaves.emplace_back(queryPlan);
    return *this;
}

DataSourcePtr GeneratedQueryExecutionPlanBuilder::getSource(size_t index) {
    return sources[index];
}

DataSinkPtr GeneratedQueryExecutionPlanBuilder::getSink(size_t index) {
    return sinks[index];
}

size_t GeneratedQueryExecutionPlanBuilder::getNumberOfPipelineStages() const {
    return stages.size();
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::setQuerySubPlanId(QuerySubPlanId querySubPlanId) {
    this->querySubPlanId = querySubPlanId;
    return *this;
}

QuerySubPlanId GeneratedQueryExecutionPlanBuilder::getQuerySubPlanId() const {
    return querySubPlanId;
}
GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::setWinDef(const WindowDefinitionPtr& winDef) {
    this->winDef = winDef;
    return *this;
}

GeneratedQueryExecutionPlanBuilder& GeneratedQueryExecutionPlanBuilder::setSchema(const SchemaPtr& schema) {
    this->schema = schema;
    return *this;
}
}// namespace NES