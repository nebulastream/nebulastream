#include <REST/testsForOatpp/TestControllerTest.hpp>
#include <iostream>

void runTests() {

    OATPP_RUN_TEST(TestControllerTest);

    // TODO - Add more tests here:
    // OATPP_RUN_TEST(MyAnotherTest);

}

int main() {

    oatpp::base::Environment::init();

    runTests();

    /* Print how much objects were created during app running, and what have left-probably leaked */
    /* Disable object counting for release builds using '-D OATPP_DISABLE_ENV_OBJECT_COUNTERS' flag for better performance */
    std::cout << "\nEnvironment:\n";
    std::cout << "objectsCount = " << oatpp::base::Environment::getObjectsCount() << "\n";
    std::cout << "objectsCreated = " << oatpp::base::Environment::getObjectsCreated() << "\n\n";

    OATPP_ASSERT(oatpp::base::Environment::getObjectsCount() == 0);

    oatpp::base::Environment::destroy();

    return 0;
}
