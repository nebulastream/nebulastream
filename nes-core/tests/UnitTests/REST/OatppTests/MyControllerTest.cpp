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

/***************************************************************************
 *
 * Project         _____    __   ____   _      _
 *                (  _  )  /__\ (_  _)_| |_  _| |_
 *                 )(_)(  /(__)\  )( (_   _)(_   _)
 *                (_____)(__)(__)(__)  |_|    |_|
 *
 *
 * Copyright 2018-present, Leonid Stryzhevskyi <lganzzzo@gmail.com>
 *
 ***************************************************************************/

#include <../../tests/UnitTests/REST/OatppTests/MyControllerTest.hpp>
#include <oatpp/web/client/HttpRequestExecutor.hpp>
#include <oatpp-test/web/ClientServerTestRunner.hpp>
#include <REST/OatppController/TestController.hpp>
#include <../../tests/UnitTests/REST/OatppTests/MyApiTestClient.hpp>
#include <../../tests/UnitTests/REST/OatppTests/TestComponent.hpp>
#include <iostream>

MyControllerTest::MyControllerTest() : UnitTest("TEST[MyControllerTest]" /* Test TAG for logs */){};

void MyControllerTest::onRun() {

    /* Register test components */
    TestComponent component;

    /* Create client-server test runner */
    oatpp::test::web::ClientServerTestRunner runner;

    /* Add TestController endpoints to the router of the test server */
    runner.addController(std::make_shared<TestController>());

    /* Run test */
    runner.run(
        [ ] {
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

            /* Read response body as ConnectivityResponse */
            auto message = response->readBodyToDto<oatpp::Object<ConnectivityResponse>>(objectMapper.get());

            /* Assert that received message is as expected */
            OATPP_ASSERT(message);
            OATPP_ASSERT(message->statusCode == 200);
            OATPP_ASSERT(message->message == "Hello World!");
        },
        std::chrono::minutes(10) /* test timeout */);

    /* wait all server threads finished */
    std::this_thread::sleep_for(std::chrono::seconds(1));
}