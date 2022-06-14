#ifndef NES_NES_CORE_INCLUDE_REST_TESTSFOROATPP_TESTCONTROLLERTEST_HPP_
#define NES_NES_CORE_INCLUDE_REST_TESTSFOROATPP_TESTCONTROLLERTEST_HPP_

#include <oatpp-test/UnitTest.hpp>

class TestControllerTest : public oatpp::test::UnitTest {
  public:

    TestControllerTest() : UnitTest("TEST[TestControllerTest]" /* Test TAG for logs */){}
    void onRun() override;

};

#endif//NES_NES_CORE_INCLUDE_REST_TESTSFOROATPP_TESTCONTROLLERTEST_HPP_
