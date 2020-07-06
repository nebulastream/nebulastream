#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATEDQUERYEXECUTIONPLANBUILDER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATEDQUERYEXECUTIONPLANBUILDER_HPP_

#include <QueryCompiler/QueryExecutionPlanId.hpp>
#include <map>
#include <memory>
#include <vector>
#include <Common/ForwardDeclaration.hpp>

namespace NES {

/**
 * @brief This GeneratedQueryExecutionPlanBuilder is a mutable object that allows constructing
 * immutable QueryExecutionPlan using the builder pattern.
 */
class GeneratedQueryExecutionPlanBuilder {
  public:
    /**
     * @brief Creates and returns an empty qep builder
     */
    static GeneratedQueryExecutionPlanBuilder create();

    /**
     * @brief add pipeline stage to plan
     * @param pipelineStagePtr
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& addPipelineStage(PipelineStagePtr pipelineStagePtr);

    /**
     * @brief configure buffer manager
     * @param bufferManager
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& setBufferManager(BufferManagerPtr bufferManager);

    /**
     * @brief returns currently set buffer manager for the builder
     * @return currently set buffer manager for the builder
     */
    BufferManagerPtr getBufferManager() const;

    /**
     * @brief configure query manager
     * @param queryManager
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& setQueryManager(QueryManagerPtr queryManager);

    /**
     * @return currently set query manager for the builder
     */
    QueryManagerPtr getQueryManager() const;

    /**
     * @return total number of pipeline stagers in the qep
     */
    size_t getNumberOfPipelineStages() const;

    /**
     * @param index
     * @return the index-th data source
     */
    DataSourcePtr getSource(size_t index);

    /**
    * @param index
    * @return the index-th data sink
    */
    DataSinkPtr getSink(size_t index);

    /**
     * @return all sinks in the plan
     */
    std::vector<DataSinkPtr>& getSinks();

    /**
     * @return the query execution plan id
     */
    QueryExecutionPlanId getQueryId() const;

    /**
     * @return a query execution plan with the specified configuration
     */
    QueryExecutionPlanPtr build();

    /**
     * @brief configure query compiler
     * @param queryCompiler
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& setCompiler(QueryCompilerPtr queryCompiler);

    /**
     * @brief configure the query execution plan id
     * @param queryId
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& setQueryId(std::string queryId);

    /**
     * @brief add operator to the plan (check compiler/code generator documentation)
     * @param operatorPtr
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& addOperatorQueryPlan(OperatorPtr operatorPtr);

    /**
     * @brief add data source
     * @param source
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& addSource(DataSourcePtr source);

    /**
     * @brief add data sink
     * @param sink
     * @return this
     */
    GeneratedQueryExecutionPlanBuilder& addSink(DataSinkPtr sink);


  private:
    GeneratedQueryExecutionPlanBuilder();

  private:
    QueryExecutionPlanId queryId;
    BufferManagerPtr bufferManager;
    QueryManagerPtr queryManager;
    QueryCompilerPtr queryCompiler;
    std::vector<DataSourcePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<PipelineStagePtr> stages;
    std::vector<OperatorPtr> leaves;
};
}
#endif//NES_INCLUDE_QUERYCOMPILER_GENERATEDQUERYEXECUTIONPLANBUILDER_HPP_
