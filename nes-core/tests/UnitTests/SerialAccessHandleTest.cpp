#include <NesBaseTest.hpp>
#include <Topology/Topology.hpp>
#include <WorkQueues/SerialStorageAccessHandle.hpp>
#include <Topology/TopologyNode.hpp>

namespace NES {
class SerialAccessHandleTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SerialAccessHandleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SerialAccessHandle test class.");
    }
};

TEST_F(SerialAccessHandleTest, TestResourceAccess) {
    auto topology = Topology::create();
    auto serialAccessHandle = SerialStorageAccessHandle::create(topology);
    ASSERT_EQ(topology.get(), serialAccessHandle->getTopologyHandle().get());
    ASSERT_EQ(topology->getRoot(), serialAccessHandle->getTopologyHandle()->getRoot());
}

}
