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
#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <Util/Files.hpp>
#include <gtest/gtest.h>

namespace NES
{

TEST(CreateUniqueFileTest, CreatesWritableFileInCurrentDirectoryWithPrefixAndSuffix)
{
    auto [stream, path] = createUniqueFile("myprefix", ".csv");
    auto filename = path.filename().string();

    EXPECT_TRUE(stream.is_open());
    EXPECT_EQ(std::filesystem::current_path(), std::filesystem::absolute(path).parent_path());
    EXPECT_TRUE(filename.starts_with("myprefix"));
    EXPECT_TRUE(filename.ends_with(".csv"));

    stream << "hello";
    stream.close();

    std::ifstream readBack(path);
    std::string content;
    readBack >> content;
    EXPECT_EQ(content, "hello");

    std::filesystem::remove(path);
}

TEST(CreateUniqueFileTest, MultipleCallsProduceUniqueFiles)
{
    constexpr int numFiles = 10;
    std::set<std::filesystem::path> paths;
    for (int i = 0; i < numFiles; ++i)
    {
        auto [stream, path] = createUniqueFile("unique", ".tmp");
        paths.insert(path);
        stream.close();
    }
    EXPECT_EQ(paths.size(), numFiles);
    for (const auto& path : paths)
    {
        std::filesystem::remove(path);
    }
}

}
