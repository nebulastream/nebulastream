#include <iostream>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <s2/s2point.h>
#include <s2/s2latlng.h>

using std::cout;
using std::endl;
#define DEBUG_OUTPUT
namespace NES {

class s2libraryTests : public testing::Test {

  public:
    static void SetUpTestCase() {
        NES::setupLogging("s2LibraryTests.log", NES::LOG_DEBUG);
        NES_INFO("Setup s2librarytest test class.");
    }


    void TearDown() override { std::cout << "Tear down s2librarytest class." << std::endl; }
};

TEST_F(s2libraryTests, textEQandNEQoperators) {
    S2LatLng cmp = S2LatLng::FromDegrees(2.1, -1.1);
    S2LatLng eq  = S2LatLng::FromDegrees(2.1, -1.1);
    S2LatLng neq = S2LatLng::FromDegrees(2.1, -1.0);

    //test if the modified equality operators work
    EXPECT_EQ(cmp, cmp);
    EXPECT_EQ(cmp, eq);
    EXPECT_NE(cmp, neq);

}
}
