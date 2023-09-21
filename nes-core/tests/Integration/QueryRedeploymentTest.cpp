#include <BaseIntegrationTest.hpp>

namespace NES {
class QueryRedeploymentTest : public Testing::BaseIntegrationTest {

    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryRedeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("SetUpTestCase QueryRedeploymentTest");
    }
};
}
