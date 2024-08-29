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

#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>
#include <tuple>

namespace NES {

using namespace Configurations;

class CoordinatorRPCServerTest : public Testing::BaseUnitTest {
  public:
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        VALID_LOGICAL_SOURCE_SCHEMA = TestSchemas::getSchemaTemplate("id_val_u64");
    }

    static void SetUpTestCase() { setupLogging(); }
    constexpr static auto VALID_LOGICAL_SOURCE_NAME = "ValidSource";
    constexpr static auto INVALID_LOGICAL_SOURCE_NAME = "InvalidSource";

  protected:
    SchemaPtr VALID_LOGICAL_SOURCE_SCHEMA;
    static void setupLogging() {
        NES::Logger::setupLogging("ExpressionNodeTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup ExpressionNodeTest test class.");
    }
    static std::tuple<std::unique_ptr<CoordinatorRPCServer>, std::shared_ptr<Catalogs::Source::SourceCatalog>> defaultUUT() {
        CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
        NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
        auto requestHandlerService = crd->getRequestHandlerService();
        auto sourceCatalog = crd->getSourceCatalog();

        return std::make_tuple(
            std::make_unique<CoordinatorRPCServer>(requestHandlerService, nullptr, nullptr, nullptr, nullptr, nullptr),
            sourceCatalog);
    }
};

TEST_F(CoordinatorRPCServerTest, testEmptyPhysicalSourceRegistrationRequest) {
    auto [uut, _] = defaultUUT();
    ServerContext serverContext;
    RegisterPhysicalSourcesRequest request;
    RegisterPhysicalSourcesReply reply;

    uut->RegisterPhysicalSource(&serverContext, &request, &reply);

    EXPECT_THAT(reply.results(), ::testing::IsEmpty());
    EXPECT_TRUE(reply.success());
}

TEST_F(CoordinatorRPCServerTest, testValidPhysicalSourceRegistrationRequest) {
    auto [uut, sourceCatalog] = defaultUUT();

    sourceCatalog->addLogicalSource(VALID_LOGICAL_SOURCE_NAME, VALID_LOGICAL_SOURCE_SCHEMA);

    ServerContext serverContext;
    RegisterPhysicalSourcesRequest request;
    RegisterPhysicalSourcesReply reply;

    request.set_workerid(2);
    PhysicalSourceDefinition* physicalSourceDefinition = request.add_physicalsourcetypes();
    physicalSourceDefinition->set_sourcetype(std::string(magic_enum::enum_name(SourceType::TCP_SOURCE)));
    physicalSourceDefinition->set_physicalsourcename(fmt::format("{}_{}", VALID_LOGICAL_SOURCE_NAME, 1));
    physicalSourceDefinition->set_logicalsourcename(VALID_LOGICAL_SOURCE_NAME);

    uut->RegisterPhysicalSource(&serverContext, &request, &reply);

    EXPECT_THAT(reply.results(), ::testing::SizeIs(1));
    EXPECT_EQ(reply.results(0).physicalsourcename(), fmt::format("{}_{}", VALID_LOGICAL_SOURCE_NAME, 1));
    EXPECT_THAT(reply.results(0).reason(), ::testing::IsEmpty());
    EXPECT_TRUE(reply.results(0).success());
    EXPECT_TRUE(reply.success());
}

TEST_F(CoordinatorRPCServerTest, testInvalidPhysicalSourceRegistrationRequest) {
    auto [uut, sourceCatalog] = defaultUUT();

    sourceCatalog->addLogicalSource(VALID_LOGICAL_SOURCE_NAME, VALID_LOGICAL_SOURCE_SCHEMA);

    ServerContext serverContext;
    RegisterPhysicalSourcesRequest request;
    RegisterPhysicalSourcesReply reply;

    request.set_workerid(2);
    PhysicalSourceDefinition* physicalSourceDefinition = request.add_physicalsourcetypes();
    physicalSourceDefinition->set_sourcetype(std::string(magic_enum::enum_name(SourceType::TCP_SOURCE)));
    physicalSourceDefinition->set_physicalsourcename(fmt::format("{}_{}", INVALID_LOGICAL_SOURCE_NAME, 1));
    physicalSourceDefinition->set_logicalsourcename(INVALID_LOGICAL_SOURCE_NAME);

    uut->RegisterPhysicalSource(&serverContext, &request, &reply);

    EXPECT_THAT(reply.results(), ::testing::SizeIs(1));
    EXPECT_EQ(reply.results(0).physicalsourcename(), fmt::format("{}_{}", INVALID_LOGICAL_SOURCE_NAME, 1));
    EXPECT_THAT(reply.results(0).reason(), ::testing::Not(::testing::IsEmpty()));
    EXPECT_FALSE(reply.results(0).success());
    EXPECT_FALSE(reply.success());
}

TEST_F(CoordinatorRPCServerTest, testValidAndInvalidPhysicalSourceRegistrationRequest) {
    auto [uut, sourceCatalog] = defaultUUT();

    sourceCatalog->addLogicalSource(VALID_LOGICAL_SOURCE_NAME, VALID_LOGICAL_SOURCE_SCHEMA);

    ServerContext serverContext;
    RegisterPhysicalSourcesRequest request;
    RegisterPhysicalSourcesReply reply;

    request.set_workerid(2);
    {
        PhysicalSourceDefinition* physicalSourceDefinition = request.add_physicalsourcetypes();
        physicalSourceDefinition->set_sourcetype(std::string(magic_enum::enum_name(SourceType::TCP_SOURCE)));
        physicalSourceDefinition->set_physicalsourcename(fmt::format("{}_{}", INVALID_LOGICAL_SOURCE_NAME, 1));
        physicalSourceDefinition->set_logicalsourcename(INVALID_LOGICAL_SOURCE_NAME);
    }
    {
        PhysicalSourceDefinition* physicalSourceDefinition = request.add_physicalsourcetypes();
        physicalSourceDefinition->set_sourcetype(std::string(magic_enum::enum_name(SourceType::TCP_SOURCE)));
        physicalSourceDefinition->set_physicalsourcename(fmt::format("{}_{}", VALID_LOGICAL_SOURCE_NAME, 1));
        physicalSourceDefinition->set_logicalsourcename(VALID_LOGICAL_SOURCE_NAME);
    }

    uut->RegisterPhysicalSource(&serverContext, &request, &reply);

    EXPECT_THAT(reply.results(), ::testing::SizeIs(2));
    EXPECT_EQ(reply.results(0).physicalsourcename(), fmt::format("{}_{}", INVALID_LOGICAL_SOURCE_NAME, 1));
    EXPECT_THAT(reply.results(0).reason(), ::testing::Not(::testing::IsEmpty()));
    EXPECT_FALSE(reply.results(0).success());

    EXPECT_EQ(reply.results(1).physicalsourcename(), fmt::format("{}_{}", VALID_LOGICAL_SOURCE_NAME, 1));
    EXPECT_THAT(reply.results(1).reason(), ::testing::IsEmpty());
    EXPECT_TRUE(reply.results(1).success());

    EXPECT_THAT(sourceCatalog->getPhysicalSources(VALID_LOGICAL_SOURCE_NAME), ::testing::IsEmpty())
        << "Expected physical sources to be unregistered on partial failure";

    EXPECT_FALSE(reply.success());
}

}// namespace NES
