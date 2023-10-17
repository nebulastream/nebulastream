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
#include  <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include  <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Exceptions/RequestExecutionException.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {
using Exceptions::InvalidQueryStateException;
using Exceptions::QueryNotFoundException;
using Exceptions::RequestExecutionException;

class RequestExecutionExceptionTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Config.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup RequestExecutionException test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down RequestExecutionException test class."); }
};

/**
 * @brief Checks if the instanceOf() function returns true only for the actual class of the object
 */
TEST_F(RequestExecutionExceptionTest, testInstanceOf) {
    InvalidQueryStateException invalidQueryStatusException({QueryState::STOPPED}, QueryState::MARKED_FOR_FAILURE);
    RequestExecutionException& test1 = invalidQueryStatusException;
    ASSERT_TRUE(test1.instanceOf<InvalidQueryStateException>());
    ASSERT_FALSE(test1.instanceOf<QueryNotFoundException>());
    ASSERT_FALSE(test1.instanceOf<Exceptions::QueryUndeploymentException>());

    QueryNotFoundException queryNotFoundException("not found");
    RequestExecutionException& test2 = queryNotFoundException;
    ASSERT_FALSE(test2.instanceOf<InvalidQueryStateException>());
    ASSERT_TRUE(test2.instanceOf<QueryNotFoundException>());
    ASSERT_FALSE(test2.instanceOf<Exceptions::QueryUndeploymentException>());

    Exceptions::QueryUndeploymentException queryUndeploymentException(1, "could not undeploy");
    RequestExecutionException& test3 = queryUndeploymentException;
    ASSERT_FALSE(test3.instanceOf<InvalidQueryStateException>());
    ASSERT_FALSE(test3.instanceOf<QueryNotFoundException>());
    ASSERT_TRUE(test3.instanceOf<Exceptions::QueryUndeploymentException>());
}
}// namespace NES