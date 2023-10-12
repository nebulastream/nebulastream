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

#include <BaseIntegrationTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <Statistics/StatCollectors/StatCollectorParamObjFactory.hpp>
#include <Statistics/StatCollectors/StatCollectorParamObjects/StatCollectorBuildParamObj.hpp>
#include <Statistics/StatCollectors/Synopses/Sketches/CountMin/CountMinParamObjects/CountMinBuildParamObj.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>

namespace NES {
	class StatisticsTest : public Testing::BaseUnitTest {
	public:
		static void SetUpTestCase() {
			NES::Logger::setupLogging("StatisticsTest.log", NES::LogLevel::LOG_DEBUG);

			NES_INFO("Setup StatisticsTest test class.");
		}

		void SetUp() override {
			Testing::BaseUnitTest::SetUp();
			statCollectorParamObjFactory = new NES::Experimental::Statistics::StatCollectorParamObjFactory();
		}

		static void TearDownTestCase() { NES_DEBUG("Tear down StateTest test class."); }

		NES::Experimental::Statistics::StatCollectorParamObjFactory *statCollectorParamObjFactory;
	};

	TEST_F(StatisticsTest, testParamObjFactory) {

		std::string defaultLogicalStreamName = "defaultLogicalStreamName";
		std::string defaultPhysicalStreamName = "defaultPhysicalStreamName";
		std::string defaultFieldName = "defaultFieldName";

		auto abstractBuildParamObj = statCollectorParamObjFactory->createCountMinBuildParamObj(defaultLogicalStreamName,
																																													 defaultPhysicalStreamName,
																																													 defaultFieldName);
		auto countMinBuildParamObj = std::static_pointer_cast<NES::Experimental::Statistics::CountMinBuildParamObj>(abstractBuildParamObj);

		auto defaultCoordConfig = Configurations::CoordinatorConfiguration::createDefault();

		bool allSetCorrectly = true;
		if (defaultCoordConfig->synopsisWindowSize != countMinBuildParamObj->getWindowSize()) {
			allSetCorrectly = false;
		} else if (defaultCoordConfig->synopsisError != countMinBuildParamObj->getError()) {
			allSetCorrectly = false;
		} else if (defaultCoordConfig->synopsisProb != countMinBuildParamObj->getProb()) {
			allSetCorrectly = false;
		} else if (defaultLogicalStreamName != countMinBuildParamObj->getLogicalSourceName()) {
			allSetCorrectly = false;
		} else if (defaultPhysicalStreamName != countMinBuildParamObj->getPhysicalSourceName()) {
			allSetCorrectly = false;
		} else if (defaultFieldName != countMinBuildParamObj->getFieldName()) {
			allSetCorrectly = false;
		}
		EXPECT_EQ(true, allSetCorrectly);
	}
}