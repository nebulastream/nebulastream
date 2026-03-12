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

#include "Rules/RuleManager.hpp"


#include <cstdint>
#include <memory>
#include <vector>

#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>

namespace NES
{
namespace
{

class RuleManagerTest : public ::testing::Test
{
};


using TestRule = Rule<std::nullptr_t>;
template <typename T>
concept PlanRuleConcept = RuleConcept<T, std::nullptr_t>;

class FirstTestRule
{
public:
    [[nodiscard]] const std::type_info& getType() const { return typeid(FirstTestRule); }
    [[nodiscard]] std::string_view getName() const { return "FirstTestRule"; }
    [[nodiscard]] std::set<std::type_index> getDependencies() const { return {}; }
    [[nodiscard]] std::nullptr_t apply(std::nullptr_t) const { return nullptr; };
    bool operator==(const FirstTestRule&) const { return false;};
};

static_assert(PlanRuleConcept<FirstTestRule>);

class SecondTestRule
{
public:
    [[nodiscard]] const std::type_info& getType() const { return typeid(SecondTestRule); }
    [[nodiscard]] std::string_view getName() const { return "SecondTestRule"; }
    [[nodiscard]] std::set<std::type_index> getDependencies() const { return {typeid(FirstTestRule)}; }
    [[nodiscard]] std::nullptr_t apply(std::nullptr_t) const { return nullptr; };
    bool operator==(const SecondTestRule&) const { return false; };
};

static_assert(PlanRuleConcept<SecondTestRule>);

class ThirdTestRule
{
public:
    [[nodiscard]] const std::type_info& getType() const { return typeid(ThirdTestRule);}
    [[nodiscard]] std::string_view getName() const { return "ThirdTestRule"; }
    [[nodiscard]] std::set<std::type_index> getDependencies() const { return {typeid(FirstTestRule)}; }
    [[nodiscard]] std::nullptr_t apply(std::nullptr_t) const { return nullptr; };
    bool operator==(const ThirdTestRule&) const { return false; };
};

static_assert(PlanRuleConcept<ThirdTestRule>);

class FourthTestRule
{
public:
    [[nodiscard]] const std::type_info& getType() const { return typeid(FourthTestRule);}
    [[nodiscard]] std::string_view getName() const { return "FourthTestRule"; }
    [[nodiscard]] std::set<std::type_index> getDependencies() const
    {
        return {typeid(SecondTestRule), typeid(ThirdTestRule)};
    }
    [[nodiscard]] std::nullptr_t apply(std::nullptr_t) const { return nullptr; };
    bool operator==(const FourthTestRule&) const { return false; };
};

static_assert(PlanRuleConcept<FourthTestRule>);

TEST_F(RuleManagerTest, Sequencing)
{
    RuleManager<std::nullptr_t> manager;

    manager.addRule(ThirdTestRule{});
    manager.addRule(FourthTestRule{});
    manager.addRule(FirstTestRule{});
    manager.addRule(SecondTestRule{});

    auto seq = manager.getSequence();

    ASSERT_EQ(seq.size(), 4);
    ASSERT_EQ(seq.at(0).getType(), typeid(FirstTestRule));
    ASSERT_EQ(seq.at(1).getType(), typeid(ThirdTestRule));
    ASSERT_EQ(seq.at(2).getType(), typeid(SecondTestRule));
    ASSERT_EQ(seq.at(3).getType(), typeid(FourthTestRule));
}

}
}
