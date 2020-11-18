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

#ifndef INCLUDE_QUERYCOMPILER_QUERYCOMPILER_HPP_
#define INCLUDE_QUERYCOMPILER_QUERYCOMPILER_HPP_
#include <QueryCompiler/GeneratedQueryExecutionPlanBuilder.hpp>
#include <memory>
#include <vector>
namespace NES {
class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class QueryCompiler;
typedef std::shared_ptr<QueryCompiler> QueryCompilerPtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

class GeneratableOperator;
typedef std::shared_ptr<GeneratableOperator> GeneratableOperatorPtr;

/**
 * @brief The query compiler compiles physical query plans to an executable query plan
 */
class QueryCompiler {
  public:
    QueryCompiler();
    ~QueryCompiler() = default;
    /**
     * Creates a new query compiler
     * @param codeGenerator
     * @return
     */
    static QueryCompilerPtr create();
    /**
     * @brief compiles a queryplan to a executable query plan
     * @param queryPlan
     * @return
     */
    void compile(GeneratedQueryExecutionPlanBuilder&, OperatorNodePtr queryPlan);

  private:
    void compilePipelineStages(GeneratedQueryExecutionPlanBuilder& queryExecutionPlanBuilder, CodeGeneratorPtr codeGenerator,
                               PipelineContextPtr context);
};

QueryCompilerPtr createDefaultQueryCompiler();

}// namespace NES

#endif//INCLUDE_QUERYCOMPILER_QUERYCOMPILER_HPP_
