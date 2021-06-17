/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Configurations/ConfigOption.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>
#include <Util/TestUtils.hpp>
#include <boost/process.hpp>
#include <cpprest/filestream.h>
#include <cpprest/http_client.h>
#include <cstdio>
#include <filesystem>
#include <sstream>

using namespace utility;
// Common utilities like string conversions
using namespace web;
// Common features like URIs.
using namespace web::http;
// Common HTTP functionality
using namespace web::http::client;
// HTTP client features
using namespace concurrency::streams;
// Asynchronous streams
namespace bp = boost::process;

namespace NES {

class ConfigTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("Config.log", NES::LOG_DEBUG);
        NES_INFO("Setup Configuration test class.");
    }

    void SetUp() override {

    }

    static void TearDownTestCase() { NES_INFO("Tear down ActorCoordinatorWorkerTest test class."); }
};

/**
 * @brief This test starts two workers and a coordinator and submit the same query but will output the results in different files
 */
TEST_F(ConfigTest, testEmptyParamsAndMissingParamsCoordinatorYAMLFile) {

    CoordinatorConfigPtr coordinatorConfigPtr = CoordinatorConfig::create();
    coordinatorConfigPtr->overwriteConfigWithYAMLFileInput("../tests/test_data/emptyCoordinator.yaml");

    EXPECT_EQ(coordinatorConfigPtr->getRestPort()->getValue(), coordinatorConfigPtr->getRestPort()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getRpcPort()->getValue(), coordinatorConfigPtr->getRpcPort()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getDataPort()->getValue(), coordinatorConfigPtr->getDataPort()->getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->getRestIp()->getValue(), coordinatorConfigPtr->getRestIp()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getCoordinatorIp()->getValue(), coordinatorConfigPtr->getCoordinatorIp()->getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->getNumberOfSlots()->getValue(), coordinatorConfigPtr->getNumberOfSlots()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getEnableQueryMerging()->getValue(), coordinatorConfigPtr->getEnableQueryMerging()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getLogLevel()->getValue(), coordinatorConfigPtr->getLogLevel()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getNumberOfBuffersInGlobalBufferManager()->getValue(), coordinatorConfigPtr->getNumberOfBuffersInGlobalBufferManager()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getNumberOfBuffersPerPipeline()->getValue(), coordinatorConfigPtr->getNumberOfBuffersPerPipeline()->getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->getNumberOfBuffersInSourceLocalBufferPool()->getValue(), coordinatorConfigPtr->getNumberOfBuffersInSourceLocalBufferPool()->getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->getBufferSizeInBytes()->getValue(), coordinatorConfigPtr->getBufferSizeInBytes()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getNumWorkerThreads()->getValue(), coordinatorConfigPtr->getNumWorkerThreads()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getQueryBatchSize()->getValue(), coordinatorConfigPtr->getQueryBatchSize()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getQueryMergerRule()->getValue(), coordinatorConfigPtr->getQueryMergerRule()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getEnableSemanticQueryValidation()->getValue(), coordinatorConfigPtr->getEnableSemanticQueryValidation()->getDefaultValue());

}

}// namespace NES