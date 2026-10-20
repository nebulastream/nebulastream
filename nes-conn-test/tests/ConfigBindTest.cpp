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

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <Plugins/BuiltinPlugins.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <Binder.hpp>

/// Binds every config of one connector exactly as it is committed, the way a user who copies it would.
/// The battery binds the same files with the endpoint rewritten, so it never sees them unmodified.
/// ctest runs one instance per connector and passes its config directory in CONNTEST_CONFIG_DIR.

namespace NES::ConnTest
{
namespace
{
std::vector<std::filesystem::path> configFiles(const std::filesystem::path& directory)
{
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(directory))
    {
        if (entry.is_regular_file() && entry.path().extension() == ".nesql")
        {
            files.push_back(entry.path());
        }
    }
    std::ranges::sort(files);
    return files;
}

std::string readFile(const std::filesystem::path& path)
{
    const std::ifstream file(path);
    std::stringstream content;
    content << file.rdbuf();
    return content.str();
}

class ConfigBindTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("ConfigBindTest.log", LogLevel::LOG_DEBUG);
        loadBuiltinPlugins();
    }
};
}

TEST_F(ConfigBindTest, EveryCommittedConfigBinds)
{
    const char* directory = std::getenv("CONNTEST_CONFIG_DIR"); /// NOLINT(concurrency-mt-unsafe)
    ASSERT_NE(directory, nullptr) << "CONNTEST_CONFIG_DIR is not set. ctest sets it per connector, see add_conn_test.";
    const auto files = configFiles(directory);
    ASSERT_FALSE(files.empty()) << "no .nesql config under " << directory;
    for (const auto& file : files)
    {
        SCOPED_TRACE(file.string());
        const auto descriptor = bindSourceStatements(readFile(file));
        EXPECT_TRUE(descriptor.has_value()) << descriptor.error().message;
    }
}

}
