/*
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
    return DataSinkProvider::lower(sinkId,
                                   sinkDescriptor,
                                   schema,
                                   nodeEngine,
                                   querySubPlan,
                                   numOfProducers);
}

}// namespace NES::TestUtils
