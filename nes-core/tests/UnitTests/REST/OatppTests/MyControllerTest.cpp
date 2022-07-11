#include <REST/OatppController/TestController.hpp>
#include <../../tests/UnitTests/REST/OatppTests/MyApiTestClient.hpp>
#include <../../tests/UnitTests/REST/OatppTests/TestComponent.hpp>
#include <../../tests/UnitTests/REST/OatppTests/MyControllerTest.hpp>

#include <oatpp/web/client/HttpRequestExecutor.hpp>
#include <oatpp-test/web/ClientServerTestRunner.hpp>


MyControllerTest::MyControllerTest() : UnitTest("TEST[MyControllerTest]" /* Test TAG for logs */){};
MyControllerTest::~MyControllerTest() = default;

void MyControllerTest::onRun() {

    /* Register test components */
    TestComponent component;

    /* Create client-server test runner */
    oatpp::test::web::ClientServerTestRunner runner;

    /* Add TestController endpoints to the router of the test server */
    runner.addController(std::make_shared<TestController>());

    /* Run test */
    runner.run(
        [this, &runner] {
            /* Get client connection provider for Api Client */
            OATPP_COMPONENT(std::shared_ptr<oatpp::network::ClientConnectionProvider>, clientConnectionProvider);

            /* Get object mapper component */
            OATPP_COMPONENT(std::shared_ptr<oatpp::data::mapping::ObjectMapper>, objectMapper);

            /* Create http request executor for Api Client */
            auto requestExecutor = oatpp::web::client::HttpRequestExecutor::createShared(clientConnectionProvider);

            /* Create Test API client */
            auto client = MyApiTestClient::createShared(requestExecutor, objectMapper);

            /* Call server API */
            /* Call hello endpoint of TestController */
            auto response = client->getHello();

            /* Assert that server responds with 200 */
            OATPP_ASSERT(response->getStatusCode() == 200);

            /* Read response body as MessageDto */
            auto message = response->readBodyToDto<oatpp::Object<MessageDto>>(objectMapper.get());

            /* Assert that received message is as expected */
            OATPP_ASSERT(message);
            OATPP_ASSERT(message->statusCode->getValue() == 200);
            OATPP_ASSERT(message->message == "Hello World!");
        },
        std::chrono::minutes(10) /* test timeout */);

    /* wait all server threads finished */
    std::this_thread::sleep_for(std::chrono::seconds(1));
}