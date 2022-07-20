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

#ifndef NES_NES_CORE_INCLUDE_REST_TESTSFOROATPP_TESTCONTROLLERTEST_HPP_
#define NES_NES_CORE_INCLUDE_REST_TESTSFOROATPP_TESTCONTROLLERTEST_HPP_

#include <iostream>
#include <oatpp-test/UnitTest.hpp>
#include <oatpp-test/web/ClientServerTestRunner.hpp>
#include <string>

class TestControllerTest : public oatpp::test::UnitTest {
  public:
    TestControllerTest() : UnitTest("TEST[TestControllerTest]" /* Test TAG for logs */) {}

    void onRun() override {

        oatpp::test::web::ClientServerTestRunner runner;
        /* Run test */
        runner.run(
            [this, &runner] {
                std::cout << this->TAG;
                std::cout << runner.getServer()->getStatus();
                std::cout << "Me";
            },
            std::chrono::seconds(10));
    };
};

#endif//NES_NES_CORE_INCLUDE_REST_TESTSFOROATPP_TESTCONTROLLERTEST_HPP_
