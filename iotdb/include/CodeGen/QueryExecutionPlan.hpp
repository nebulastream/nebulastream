/*
 * QueryExecutionPlan.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_QUERYEXECUTIONPLAN_H_
#define INCLUDE_QUERYEXECUTIONPLAN_H_
#include <CodeGen/PipelineStage.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include <map>
#include <Windows/WindowHandler.hpp>
#include "../SourceSink/DataSink.hpp"
#include "../SourceSink/DataSource.hpp"


namespace iotdb {
class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class QueryExecutionPlan {
public:
    QueryExecutionPlan();

    virtual bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf);
    const std::vector<DataSourcePtr> getSources() const;
    const std::vector<WindowPtr> getWindows() const;
    const std::vector<DataSinkPtr> getSinks() const;

    uint32_t stageIdFromSource(DataSource* source);
    virtual ~QueryExecutionPlan();
    size_t getQueryResult(std::string name) { return qResult[name]; };

    void addDataSource(DataSourcePtr source) { sources.push_back(source); }

    void addDataSink(DataSinkPtr sink) { sinks.push_back(sink); }
    void addWindow(WindowPtr window) { windows.push_back(window); }

    template <class Archive> void serialize(Archive& ar, const unsigned int version)
    {
        ar& sources;
        ar& sinks;
        ar& windows;
        //    	ar & stages;
        //    	ar & source_to_stage;
        //    	ar & stage_to_dest;
        //    	ar & qResult;
    }

    void print()
    {
		for (auto source : sources) {
			IOTDB_INFO("Source:" << source)
			IOTDB_INFO("\t Generated Buffers=" << source->getNumberOfGeneratedBuffers())
			IOTDB_INFO("\t Generated Tuples=" << source->getNumberOfGeneratedTuples())
			IOTDB_INFO("\t Schema=" << source->getSourceSchemaAsString())
		}
		for (auto window : windows) {
			IOTDB_INFO("WindowHandler:" << window)
			IOTDB_INFO("WindowHandler Result:")
		}
		for (auto sink : sinks) {
			IOTDB_INFO("Sink:" << sink)
			IOTDB_INFO("\t Generated Buffers=" << sink->getNumberOfSentBuffers())
			IOTDB_INFO("\t Generated Tuples=" << sink->getNumberOfSentTuples())
		}
    }

protected:
    friend class boost::serialization::access;

    QueryExecutionPlan(const std::vector<DataSourcePtr> &_sources,
          const std::vector<PipelineStagePtr> &_stages, 
          const std::map<DataSource *, uint32_t> &_source_to_stage,
          const std::map<uint32_t, uint32_t> &_stage_to_dest);

  protected:
//    QueryExecutionPlan(const std::vector<DataSourcePtr>& _sources, const std::vector<PipelineStagePtr>& _stages,
//                       const std::map<DataSource*, uint32_t>& _source_to_stage,
//                       const std::map<uint32_t, uint32_t>& _stage_to_dest);

    std::vector<DataSourcePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<WindowPtr> windows;

    std::vector<PipelineStagePtr> stages;
    std::map<DataSource*, uint32_t> source_to_stage;
    std::map<uint32_t, uint32_t> stage_to_dest;
    std::map<std::string, size_t> qResult;
};
const QueryExecutionPlanPtr createTestQEP();

} // namespace iotdb

#endif /* INCLUDE_QUERYEXECUTIONPLAN_H_ */
