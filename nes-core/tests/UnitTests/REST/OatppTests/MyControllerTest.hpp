#ifndef NES_NES_CORE_TESTS_UNITTESTS_REST_OATPPTESTS_MYCONTROLLERTEST_HPP_
#define NES_NES_CORE_TESTS_UNITTESTS_REST_OATPPTESTS_MYCONTROLLERTEST_HPP_


#include <oatpp-test/UnitTest.hpp>

class MyControllerTest : public oatpp::test::UnitTest {
  public:

    MyControllerTest();

    void onRun() override;

};

#endif//NES_NES_CORE_TESTS_UNITTESTS_REST_OATPPTESTS_MYCONTROLLERTEST_HPP_
