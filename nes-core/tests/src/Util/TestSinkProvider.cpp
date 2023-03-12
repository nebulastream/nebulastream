#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSinkProvider.hpp>
namespace NES::TestUtils {

DataSinkPtr TestSinkProvider::lower(OperatorId sinkId,
                                    SinkDescriptorPtr sinkDescriptor,
                                    SchemaPtr schema,
                                    Runtime::NodeEnginePtr nodeEngine,
                                    const QueryCompilation::PipelineQueryPlanPtr& querySubPlan,
                                    size_t numOfProducers) {
    if (sinkDescriptor->instanceOf<TestSinkDescriptor>()) {
        auto testSinkDescriptor = sinkDescriptor->as<TestSinkDescriptor>();
        return testSinkDescriptor->getSink();
    }
    return DataSinkProvider::lower(sinkId, sinkDescriptor, schema, nodeEngine, querySubPlan, numOfProducers);
}

}// namespace NES::TestUtils
