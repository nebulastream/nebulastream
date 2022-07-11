#include "../../../../cmake-build-debug/nes-dependencies-vOatpp-cpprestsdk-x64-linux-nes/installed/x64-linux-nes/include/oatpp-1.3.0/oatpp/oatpp/web/client/ApiClient.hpp"
#include <NesBaseTest.hpp>
#include <iostream>
#include <oatpp/network/tcp/client/ConnectionProvider.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/web/client/HttpRequestExecutor.hpp>
#include <string>

#include <Components/NesCoordinator.hpp>

using namespace oatpp::network;
using namespace oatpp::web;
using namespace oatpp::parser;

namespace NES {

class TestsForOatppEndpoints : public NES::Testing::NESBaseTest, oatpp::web::client::ApiClient {

  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TestsForOatppEndpoints.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TestsForOatppEndpoints class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down TestsForOatppEndpoints test class."); }

    NesCoordinatorPtr createAndStartCoordinator() const {
        NES_INFO("TestsForOatppEndpoints: Start coordinator");
        CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinatorConfig->serverTypeOatpp = true;
        auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
        NES_INFO("TestsForOatppEndpoints: Coordinator started successfully");
        return coordinator;
    }

    StringTemplate parsePathTemplate_new(const oatpp::String& name, const oatpp::String& text) {

        std::vector<StringTemplate::Variable> variables;
        oatpp::parser::Caret caret(text);

        while(caret.canContinue()) {

            if(caret.findChar('{')) {

                caret.inc();
                auto label = caret.putLabel();
                caret.findChar('}');
                label.end();

                StringTemplate::Variable var;
                var.posStart = label.getStartPosition() - 1;
                var.posEnd = label.getEndPosition();
                var.name = label.toString();

                variables.push_back(var);

            }

        }

        StringTemplate t(text, std::move(variables));
        auto extra = std::make_shared<PathTemplateExtra>();
        t.setExtraData(extra);

        extra->name = name;

        caret.setPosition(0);
        extra->hasQueryParams = caret.findChar('?');

        return t;
    }

};

TEST_F(TestsForOatppEndpoints, testStartServerWithOatpp) {

    /* start Coordinator */
    auto coordinator = createAndStartCoordinator();
    EXPECT_TRUE(coordinator->isCoordinatorRunning());
    NES_INFO("Server started successfully with Oatpp");
}

TEST_F(TestsForOatppEndpoints, DISABLED_testControllerSimpleTest) {

    // start Coordinator
    auto coordinator = createAndStartCoordinator();

    // create connection provider
    auto connectionProvider = tcp::client::ConnectionProvider::createShared({"localhost", 8081, oatpp::network::Address::IP_4});

    // create HTTP request executor
    auto requestExecutor = client::HttpRequestExecutor::createShared(connectionProvider);

    // create JSON object mapper
    auto objectMapper = json::mapping::ObjectMapper::createShared();

    // create API client
    auto restClient = client::ApiClient::createShared(requestExecutor, objectMapper);

    // prepare executeRequest
    const oatpp::data::share::StringTemplate& pathTemplate = parsePathTemplate_new("getHello", "/hello");
    const std::shared_ptr<oatpp::web::protocol::http::outgoing::BufferBody> body = oatpp::web::protocol::http::outgoing::BufferBody::createShared("test");
    // oatpp::web::client::ApiClient::Headers& headers = oatpp::web::client::ApiClient::Headers("test");

    // get connectionHandle via RequestExecutor
    const std::shared_ptr<oatpp::web::client::RequestExecutor::ConnectionHandle>& connectionHandle = requestExecutor->getConnection();

    restClient->executeRequest("GET", pathTemplate, nullptr /*nullptr here is not possible*/, "/hello", "", body, connectionHandle);//find out how to call this method

    /*
(const oatpp::String& method,
const StringTemplate& pathTemplate,
const Headers& headers,
const std::unordered_map<oatpp::String, oatpp::String>& pathParams,
const std::unordered_map<oatpp::String, oatpp::String>& queryParams,
const std::shared_ptr<RequestExecutor::Body>& body,
const std::shared_ptr<RequestExecutor::ConnectionHandle>& connectionHandle = nullptr);
 */
}

}
