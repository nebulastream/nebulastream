#ifndef NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONREQUEST_HPP_
#define NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONREQUEST_HPP_

#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES {
namespace QueryCompilation {

class QueryCompilationRequest {
  public:
    class Builder {
      public:
        Builder(QueryPlanPtr queryPlan, NodeEngine::NodeEnginePtr nodeEngine);
        Builder& debug();
        Builder& optimze();
        Builder& dump();
        QueryCompilationRequestPtr build();
      private:
        QueryCompilationRequestPtr request;
    };
    static QueryCompilationRequestPtr create(QueryPlanPtr queryPlan, NodeEngine::NodeEnginePtr nodeEngine);
    bool isDebugEnabled();
    bool isOptimizeEnabled();
    bool isDumpEnabled();
    QueryPlanPtr getQueryPlan();
    NodeEngine::NodeEnginePtr getNodeEngine();

  private:
    QueryCompilationRequest(QueryPlanPtr queryPlan, NodeEngine::NodeEnginePtr nodeEngine);
    QueryPlanPtr queryPlan;
    NodeEngine::NodeEnginePtr nodeEngine;
    bool debug;
    bool optimize;
    bool dumpQueryPlans;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONREQUEST_HPP_
