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
#ifndef NES_INCLUDE_QUERY_COMPILER_PHASES_PIPELINING_PIPELINING_PHASE_HPP_
#define NES_INCLUDE_QUERY_COMPILER_PHASES_PIPELINING_PIPELINING_PHASE_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Pipelining phase to transform a query plan into a plan of pipelines.
 */
class PipeliningPhase {
  public:
    virtual PipelineQueryPlanPtr apply(QueryPlanPtr queryPlan) = 0;
    virtual ~PipeliningPhase() = default;
};
}// namespace QueryCompilation
}// namespace NES

#endif// NES_INCLUDE_QUERY_COMPILER_PHASES_PIPELINING_PIPELINING_PHASE_HPP_
