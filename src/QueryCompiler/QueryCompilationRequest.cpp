#include <QueryCompiler/QueryCompilationRequest.hpp>

namespace NES {
namespace QueryCompilation {

QueryCompilationRequest::Builder::Builder(QueryPlanPtr queryPlan, NodeEngine::NodeEnginePtr nodeEngine)
    : request(QueryCompilationRequest::create(queryPlan, nodeEngine)) {}

QueryCompilationRequest::Builder& QueryCompilationRequest::Builder::debug() {
    request->debug = true;
    return *this;
}

QueryCompilationRequest::Builder& QueryCompilationRequest::Builder::optimze() {
    request->debug = true;
    return *this;
}

QueryCompilationRequest::Builder& QueryCompilationRequest::Builder::dump(){
    request->dumpQueryPlans = true;
    return *this;
}

QueryCompilationRequestPtr QueryCompilationRequest::Builder::build() { return request; }

QueryCompilationRequestPtr QueryCompilationRequest::create(QueryPlanPtr queryPlan, NodeEngine::NodeEnginePtr nodeEngine) {
    return std::make_shared<QueryCompilationRequest>(QueryCompilationRequest(queryPlan, nodeEngine));
}

QueryCompilationRequest::QueryCompilationRequest(QueryPlanPtr queryPlan, NodeEngine::NodeEnginePtr nodeEngine) : queryPlan(queryPlan), nodeEngine(nodeEngine), debug(false), optimize(false), dumpQueryPlans(false) {}

bool QueryCompilationRequest::isDebugEnabled() { return debug; }

bool QueryCompilationRequest::isOptimizeEnabled() { return optimize; }

bool QueryCompilationRequest::isDumpEnabled() {
    return dumpQueryPlans;
}

QueryPlanPtr QueryCompilationRequest::getQueryPlan() { return queryPlan; }

NodeEngine::NodeEnginePtr QueryCompilationRequest::getNodeEngine() {
    return nodeEngine;
}

}// namespace QueryCompilation
}// namespace NES