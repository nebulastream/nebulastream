#ifndef INCLUDE_QUERYEXECUTIONPLAN_H_
#define INCLUDE_QUERYEXECUTIONPLAN_H_
#include <QueryCompiler/PipelineStage.hpp>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/DataSource.hpp>
#include <Windows/WindowHandler.hpp>
#include <map>

//#include <NodeEngine/QueryManager.hpp>

namespace NES {
class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class QueryExecutionPlan {
  public:
    QueryExecutionPlan(std::string queryId);
    virtual ~QueryExecutionPlan();

    /**
     * @brief Setup the query plan, e.g., instantiate state variables.
     */
    bool setup();

    /**
     * @brief Start the query plan, e.g., start window thread.
     */
    bool start();

    /**
     * @brief Stop the query plan and free all associated resources.
     */
    bool stop();

    /**
     * @brief Execute a particular pipeline stage with the given input buffer.
     * @param pipeline_stage_id
     * @param buf
     * @return true if pipeline stage was executed successfully.
     */
    virtual bool executeStage(uint32_t pipeline_stage_id, TupleBuffer& buf);

    /**
     * @brief Get pipeline stage id for a data source.
     * @param source
     * @return pipline stage id
     */
    uint32_t stageIdFromSource(DataSource* source);

    /**
     * @brief Add a data source to the query plan.
     * @param source
     */
    void addDataSource(DataSourcePtr source);

    /**
     * @brief Get data sources.
     */
    const std::vector<DataSourcePtr> getSources() const;

    /**
     * @brief Add a data sing to the query plan.
     * @param sink
     */
    void addDataSink(DataSinkPtr sink);

    /**
     * @brief Get data sinks.
     */
    const std::vector<DataSinkPtr> getSinks() const;

    /**
     * Appends a pipeline stage to the query plan.
     * @param pipelineStage
     */
    void appendPipelineStage(PipelineStagePtr pipelineStage);

    /**
     * @brief Gets number of pipeline stages.
     */
    uint32_t numberOfPipelineStages() {
        return stages.size();
    }

    QueryManagerPtr getQueryManager();
    void setQueryManager(QueryManagerPtr queryManager);

    BufferManagerPtr getBufferManager();
    void setBufferManager(BufferManagerPtr bufferManager);

    void print();

    void setQueryId(std::string queryId);
    std::string getQueryId();

  protected:
    QueryExecutionPlan(std::string queryId,
                       std::vector<DataSourcePtr> sources,
                       std::vector<PipelineStagePtr> stages,
                       std::map<DataSource*, uint32_t> sourceToStage,
                       std::map<uint32_t, uint32_t> stageToDest);

    std::vector<DataSourcePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<PipelineStagePtr> stages;
    std::map<DataSource*, uint32_t> sourceToStage;
    std::map<uint32_t, uint32_t> stageToDest;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    std::string queryId;
};

}// namespace NES

#endif /* INCLUDE_QUERYEXECUTIONPLAN_H_ */
