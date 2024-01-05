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
#include <Nautilus/Exceptions/TagCreationException.hpp>
#include <Nautilus/Tracing/Tag/TagRecorder.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace NES::Nautilus::Tracing {

class TagCreationTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TagCreationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TagCreationTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TagCreationTest test class."); }
};

TEST_F(TagCreationTest, tagCreation) {
    auto tr = TagRecorder::createTagRecorder();
    auto tag1 = tr.createTag();
    std::stringstream tag1string;
    tag1string << tag1;
    NES_INFO("{}", tag1string.str());
    auto tag2 = tr.createTag();
    std::stringstream tag2string;
    tag2string << tag2;
    NES_INFO("{}", tag2string.str());
    ASSERT_NE(tag1, tag2);
}

void createTagFunction(TagRecorder& tr, std::unordered_set<Tag*>& tags, int i) {
    if (i != 0) {
        createTagFunction(tr, tags, i - 1);
        auto tag = tr.createTag();
        tags.emplace(tag);
    }
}

TEST_F(TagCreationTest, recursiveCreateTag) {
    auto tr = TagRecorder::createTagRecorder();
    std::unordered_set<Tag*> tagMap;
#pragma clang loop unroll(disable)
    for (auto i = 0; i < 10; i++) {
        createTagFunction(tr, tagMap, 10);
    }
    ASSERT_EQ(tagMap.size(), 10);
}

TEST_F(TagCreationTest, deepRecursiveCreateTag) {
    auto tr = TagRecorder::createTagRecorder();
    std::unordered_set<Tag*> tagMap;
    ASSERT_THROW(createTagFunction(tr, tagMap, 100), TagCreationException);
}

TEST_F(TagCreationTest, tagCreationLoop) {
    auto tr = TagRecorder::createTagRecorder();
    std::unordered_map<Tag*, bool> tagMap;
    for (auto i = 0; i < 1000000; i++) {
        auto x = tr.createTag();
        tagMap.emplace(x, true);
    }
    ASSERT_EQ(tagMap.size(), 1);
}

}// namespace NES::Nautilus::Tracing