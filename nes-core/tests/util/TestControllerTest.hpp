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
