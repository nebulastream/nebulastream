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

/**
 * @brief Represents a query compilation request.
 * The request encapsulates the query plan and addition properties.
 */
class QueryCompilationRequest {
  public:
    static QueryCompilationRequestPtr create(QueryPlanPtr queryPlan, Runtime::NodeEnginePtr nodeEngine);

    /**
     * @brief Enable debugging for this query.
     */
    void enableDebugging();

    /**
     * @brief Checks if debugging is enabled
     * @return bool
     */
    bool isDebugEnabled() const;

    /**
    * @brief Checks if optimizations for this query
    */
    void enableOptimizations();

    /**
     * @brief Checks if optimization flags is enabled
     * @return bool
     */
    bool isOptimizeEnabled() const;

    /**
    * @brief Enable debugging for this query.
    */
    void enableDump();

    /**
     * @brief Checks if dumping to nesviz is enabled
     * @return bool
     */
    bool isDumpEnabled() const;

    /**
     * @brief Gets the query plan of this request
     * @return QueryPlanPtr
     */
    QueryPlanPtr getQueryPlan();

    /**
     * @brief Gets the node engine
     * @return Runtime::NodeEnginePtr
     */
    Runtime::NodeEnginePtr getNodeEngine();

  private:
    QueryCompilationRequest(QueryPlanPtr queryPlan, Runtime::NodeEnginePtr nodeEngine);
    QueryPlanPtr queryPlan;
    Runtime::NodeEnginePtr nodeEngine;
    bool debug;
    bool optimize;
    bool dumpQueryPlans;
};
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONREQUEST_HPP_
