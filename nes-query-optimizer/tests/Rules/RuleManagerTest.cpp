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

#include <Rules/RuleManager.hpp>

#include <cstddef>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <vector>
#include <Rules/Rule.hpp>
#include <gtest/gtest.h>

namespace NES
{
namespace
{

class RuleManagerTest : public ::testing::Test
{
};

template <typename T>
concept TestRuleConcept = RuleConcept<T, std::nullptr_t>;

/// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define RULE_DEPS(...) \
    std::set<std::type_index> \
    { \
        __VA_ARGS__ \
    }
/// NOLINTEND(cppcoreguidelines-macro-usage)

/// NOLINTBEGIN(bugprone-macro-parentheses)
#define DEFINE_TEST_RULE(Name, DependsOn, RequiredBy) \
    class Name \
    { \
    public: \
        [[nodiscard]] static const std::type_info& getType() \
        { \
            return typeid(Name); \
        } \
        [[nodiscard]] static std::string_view getName() \
        { \
            return #Name; \
        } \
        [[nodiscard]] std::set<std::type_index> dependsOn() const \
        { \
            return DependsOn; \
        } \
        [[nodiscard]] std::set<std::type_index> requiredBy() const \
        { \
            return RequiredBy; \
        } \
        [[nodiscard]] std::nullptr_t apply(std::nullptr_t) const \
        { \
            return nullptr; \
        } \
        bool operator==(const Name&) const \
        { \
            return false; \
        } \
    }; \
    static_assert(TestRuleConcept<Name>)
/// NOLINTEND(bugprone-macro-parentheses)

DEFINE_TEST_RULE(FirstTestRule, RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(SecondTestRule, RULE_DEPS(typeid(FirstTestRule)), RULE_DEPS());
DEFINE_TEST_RULE(FourthTestRule, RULE_DEPS(typeid(SecondTestRule)), RULE_DEPS());
DEFINE_TEST_RULE(ThirdTestRule, RULE_DEPS(typeid(FirstTestRule), typeid(SecondTestRule)), RULE_DEPS(typeid(FourthTestRule)));

TEST_F(RuleManagerTest, FailureOnAddingDuplicateRules)
{
    RuleManager<std::nullptr_t> manager;

    ASSERT_NO_THROW(manager.addRule(FirstTestRule{}));
    ASSERT_ANY_THROW(manager.addRule(FirstTestRule{}));
}

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
    ASSERT_EQ(seq.at(1).getType(), typeid(SecondTestRule));
    ASSERT_EQ(seq.at(2).getType(), typeid(ThirdTestRule));
    ASSERT_EQ(seq.at(3).getType(), typeid(FourthTestRule));

    RuleManager<std::nullptr_t> manager2;

    manager2.addRule(SecondTestRule{});
    manager2.addRule(FirstTestRule{});
    manager2.addRule(FourthTestRule{});
    manager2.addRule(ThirdTestRule{});

    auto seq2 = manager2.getSequence();

    ASSERT_EQ(seq2.size(), 4);
    ASSERT_EQ(seq2.at(0).getType(), typeid(FirstTestRule));
    ASSERT_EQ(seq2.at(1).getType(), typeid(SecondTestRule));
    ASSERT_EQ(seq2.at(2).getType(), typeid(ThirdTestRule));
    ASSERT_EQ(seq2.at(3).getType(), typeid(FourthTestRule));
}

DEFINE_TEST_RULE(CycleRule1, RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(CycleRule2, RULE_DEPS(typeid(CycleRule1)), RULE_DEPS());
DEFINE_TEST_RULE(CycleRule3, RULE_DEPS(typeid(CycleRule2)), RULE_DEPS());
DEFINE_TEST_RULE(CycleRule4, RULE_DEPS(typeid(CycleRule3)), RULE_DEPS());
DEFINE_TEST_RULE(CycleRule5, RULE_DEPS(typeid(CycleRule4)), RULE_DEPS(typeid(CycleRule3)));

TEST_F(RuleManagerTest, FailureOnCycleDependencies)
{
    RuleManager<std::nullptr_t> manager;

    manager.addRule(CycleRule1{});
    manager.addRule(CycleRule2{});
    manager.addRule(CycleRule3{});
    manager.addRule(CycleRule4{});
    manager.addRule(CycleRule5{});

    ASSERT_ANY_THROW({ auto sequence = manager.getSequence(); });
}

DEFINE_TEST_RULE(UnavailableRule, RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(AvailableRule1, RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(AvailableRule2, RULE_DEPS(typeid(AvailableRule1)), RULE_DEPS());
DEFINE_TEST_RULE(AvailableRule3, RULE_DEPS(typeid(AvailableRule2), typeid(UnavailableRule)), RULE_DEPS());

TEST_F(RuleManagerTest, FailureOnSequencingWithMissingRule)
{
    RuleManager<std::nullptr_t> manager;

    manager.addRule(AvailableRule1{});
    manager.addRule(AvailableRule2{});
    manager.addRule(AvailableRule3{});

    ASSERT_ANY_THROW({ auto sequence = manager.getSequence(); });
}


}
}
