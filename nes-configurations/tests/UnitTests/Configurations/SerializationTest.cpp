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

#include <Configurations/SerializationVisitor.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Util/Logger/Logger.hpp>

#include <BaseIntegrationTest.hpp>


namespace NES
{
using namespace Configurations;

class ConfigSerializationTest : public Testing::BaseIntegrationTest
{
public:
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("Config.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Configuration test class.");
    }
};

TEST_F(ConfigSerializationTest, test)
{
    SerializationVisitor visitor;
    WorkerConfiguration config;
    config.accept(visitor);
    visitor.print();
}

}
