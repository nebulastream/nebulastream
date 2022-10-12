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

#include <Nautilus/Tracing/BacktraceTagRecorder.hpp>
#include <Nautilus/Tracing/NativeTagRecorder.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus::Tracing {

class TagCreationTest : public testing::Test {
  public:
    std::unique_ptr<TagRecorder> tr;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TagCreationTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup TagCreationTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup TagCreationTest test case." << std::endl;
        tr = std::make_unique<BacktraceTagRecorder>();
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TagCreationTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TagCreationTest test class." << std::endl; }
};



TEST_F(TagCreationTest, tagCreation) {
    auto start = tr->createStartAddress();
    auto x = tr->createTag(start);
    auto y = tr->createTag(start);
    NES_DEBUG(x);
    ASSERT_NE(x, y);
}

void tagFunction(std::unique_ptr<TagRecorder>& tr) {
    void* (*funcPointerC)() = reinterpret_cast<void*(*)()>(tagFunction);
    auto x = tr->createTag(0);
    std::cout << (uint64_t)funcPointerC << std::endl;
    std::cout << x << std::endl;
}

TEST_F(TagCreationTest, tagCreationFunction) {
    tagFunction(tr);
}

TEST_F(TagCreationTest, tagCreationLoop) {
    auto start = tr->createStartAddress();
    std::unordered_map<Tag, bool, Tag::TagHasher> tagMap;
    for (auto i = 0; i < 1000000; i++) {
        auto x = tr->createTag(start);
        tagMap.emplace(std::make_pair(x, true));
    }
    ASSERT_EQ(tagMap.size(), 1);
}

}// namespace NES::Nautilus::Tracing