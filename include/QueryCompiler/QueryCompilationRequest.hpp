/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
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
