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

#include <BaseIntegrationTest.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <RequestProcessor/RequestTypes/StopQueryRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <gtest/gtest.h>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES::RequestProcessor::Experimental {

class StopQueryRequestTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StopQueryRequestTest.log", NES::LogLevel::LOG_TRACE);
        NES_INFO("Setup StopQueryRequestTest test class.");
    }
};
/**
 * @brief Test that the constructor of StopQueryRequest works as expected
 */
TEST_F(StopQueryRequestTest, createSimpleStopRequest) {
    constexpr QueryId queryId = 1;
    const uint8_t retries = 0;
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto stopQueryRequest = Experimental::StopQueryRequest::create(queryId, retries);
    EXPECT_EQ(stopQueryRequest->toString(), "StopQueryRequest { QueryId: " + std::to_string(queryId) + "}");
}
}// namespace NES