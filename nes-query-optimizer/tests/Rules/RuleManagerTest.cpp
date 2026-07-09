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
#include <Util/PlanRenderer.hpp>
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
#define DEFINE_TEST_RULE(Name, Needs, NeededBy, Wants, WantedBy) \
    class Name \
    { \
    public: \
        static constexpr std::string_view NAME = #Name; \
        [[nodiscard]] std::set<std::type_index> needs() const \
        { \
            return Needs; \
        } \
        [[nodiscard]] std::set<std::type_index> neededBy() const \
        { \
            return NeededBy; \
        } \
        [[nodiscard]] std::set<std::type_index> wants() const \
        { \
            return Wants; \
        } \
        [[nodiscard]] std::set<std::type_index> wantedBy() const \
        { \
            return WantedBy; \
        } \
        [[nodiscard]] std::nullptr_t apply(std::nullptr_t) const \
        { \
            return nullptr; \
        } \
    }; \
    static_assert(TestRuleConcept<Name>)
/// NOLINTEND(bugprone-macro-parentheses)

DEFINE_TEST_RULE(FirstTestRule, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(SecondTestRule, RULE_DEPS(typeid(FirstTestRule)), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(FourthTestRule, RULE_DEPS(typeid(SecondTestRule)), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(
    ThirdTestRule, RULE_DEPS(typeid(FirstTestRule), typeid(SecondTestRule)), RULE_DEPS(typeid(FourthTestRule)), RULE_DEPS(), RULE_DEPS());

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

DEFINE_TEST_RULE(CycleRule1, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(CycleRule2, RULE_DEPS(typeid(CycleRule1)), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(CycleRule3, RULE_DEPS(typeid(CycleRule2)), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(CycleRule4, RULE_DEPS(typeid(CycleRule3)), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(CycleRule5, RULE_DEPS(typeid(CycleRule4)), RULE_DEPS(typeid(CycleRule3)), RULE_DEPS(), RULE_DEPS());

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

DEFINE_TEST_RULE(UnavailableRule, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(AvailableRule1, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(AvailableRule2, RULE_DEPS(typeid(AvailableRule1)), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(AvailableRule3, RULE_DEPS(typeid(AvailableRule2), typeid(UnavailableRule)), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());

TEST_F(RuleManagerTest, FailureOnSequencingWithMissingRule)
{
    RuleManager<std::nullptr_t> manager;

    manager.addRule(AvailableRule1{});
    manager.addRule(AvailableRule2{});
    manager.addRule(AvailableRule3{});

    ASSERT_ANY_THROW({ auto sequence = manager.getSequence(); });
}

DEFINE_TEST_RULE(WantsRule1, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(WantsRule2, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(typeid(WantsRule1)), RULE_DEPS());
DEFINE_TEST_RULE(WantsRule4, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(typeid(WantsRule1), typeid(WantsRule2)), RULE_DEPS());
DEFINE_TEST_RULE(WantsRule3, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(typeid(WantsRule1), typeid(WantsRule2)), RULE_DEPS(typeid(WantsRule4)));

TEST_F(RuleManagerTest, SoftDependencies)
{
    RuleManager<std::nullptr_t> manager;

    manager.addRule(WantsRule1{});
    manager.addRule(WantsRule2{});
    manager.addRule(WantsRule3{});
    manager.addRule(WantsRule4{});

    const auto seq = manager.getSequence();

    ASSERT_EQ(seq.size(), 4);
    ASSERT_EQ(seq.at(0).getType(), typeid(WantsRule1));
    ASSERT_EQ(seq.at(1).getType(), typeid(WantsRule2));
    ASSERT_EQ(seq.at(3).getType(), typeid(WantsRule4));

    RuleManager<std::nullptr_t> manager2;
    manager2.addRule(WantsRule1{});
    manager2.addRule(WantsRule3{});
    manager2.addRule(WantsRule4{});

    const auto seq2 = manager2.getSequence();

    ASSERT_EQ(seq2.size(), 3);
    ASSERT_EQ(seq2.at(0).getType(), typeid(WantsRule1));
    ASSERT_EQ(seq2.at(1).getType(), typeid(WantsRule3));
    ASSERT_EQ(seq2.at(2).getType(), typeid(WantsRule4));
}

DEFINE_TEST_RULE(MixedRule1, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(MixedRule2, RULE_DEPS(typeid(MixedRule1)), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(MixedRule4, RULE_DEPS(typeid(MixedRule1)), RULE_DEPS(), RULE_DEPS(typeid(MixedRule2)), RULE_DEPS());
DEFINE_TEST_RULE(MixedRule5, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(
    MixedRule3, RULE_DEPS(typeid(MixedRule1)), RULE_DEPS(typeid(MixedRule4)), RULE_DEPS(typeid(MixedRule2)), RULE_DEPS(typeid(MixedRule5)));

TEST_F(RuleManagerTest, HardAndSoftDependenciesMixed)
{
    RuleManager<std::nullptr_t> manager;
    manager.addRule(MixedRule1{});
    manager.addRule(MixedRule2{});
    manager.addRule(MixedRule3{});
    manager.addRule(MixedRule4{});

    const auto seq = manager.getSequence();

    ASSERT_EQ(seq.size(), 4);
    ASSERT_EQ(seq.at(0).getType(), typeid(MixedRule1));
    ASSERT_EQ(seq.at(1).getType(), typeid(MixedRule2));
    ASSERT_EQ(seq.at(2).getType(), typeid(MixedRule3));
    ASSERT_EQ(seq.at(3).getType(), typeid(MixedRule4));


    RuleManager<std::nullptr_t> manager2;
    manager2.addRule(MixedRule1{});
    manager2.addRule(MixedRule5{});
    manager2.addRule(MixedRule3{});
    manager2.addRule(MixedRule4{});

    const auto seq2 = manager2.getSequence();

    ASSERT_EQ(seq2.size(), 4);
    ASSERT_EQ(seq2.at(0).getType(), typeid(MixedRule1));
    ASSERT_EQ(seq2.at(1).getType(), typeid(MixedRule3));
    ASSERT_EQ(seq2.at(2).getType(), typeid(MixedRule4));
    ASSERT_EQ(seq2.at(3).getType(), typeid(MixedRule5));
}

DEFINE_TEST_RULE(Unregistered1, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(Unregistered2, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());


DEFINE_TEST_RULE(ExplainRule1, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(typeid(Unregistered1)), RULE_DEPS(typeid(Unregistered2)));
DEFINE_TEST_RULE(ExplainRule2, RULE_DEPS(typeid(ExplainRule1)), RULE_DEPS(), RULE_DEPS(), RULE_DEPS());
DEFINE_TEST_RULE(ExplainRule4, RULE_DEPS(typeid(ExplainRule1)), RULE_DEPS(), RULE_DEPS(typeid(ExplainRule2)), RULE_DEPS());
DEFINE_TEST_RULE(
    ExplainRule5, RULE_DEPS(), RULE_DEPS(), RULE_DEPS(typeid(ExplainRule4), typeid(Unregistered1), typeid(Unregistered2)), RULE_DEPS());
DEFINE_TEST_RULE(
    ExplainRule3,
    RULE_DEPS(typeid(ExplainRule1)),
    RULE_DEPS(typeid(ExplainRule4)),
    RULE_DEPS(typeid(ExplainRule2)),
    RULE_DEPS(typeid(ExplainRule5)));

TEST_F(RuleManagerTest, Explain)
{
    RuleManager<std::nullptr_t> manager;

    manager.addRule(ExplainRule1{});
    manager.addRule(ExplainRule2{});
    manager.addRule(ExplainRule3{});
    manager.addRule(ExplainRule4{});
    manager.addRule(ExplainRule5{});

    ASSERT_EQ(
        manager.explain(ExplainVerbosity::Short), "RuleManager(ExplainRule1, ExplainRule2, ExplainRule3, ExplainRule4, ExplainRule5)");
    ASSERT_EQ(
        manager.explain(ExplainVerbosity::Debug),
        "RuleManager(\n"
        "\t[1] RULE(ExplainRule1) NEEDS() NEEDED_BY() WANTS(1 unregistered rule) WANTED_BY(1 unregistered rule)\n"
        "\t[2] RULE(ExplainRule2) NEEDS(ExplainRule1) NEEDED_BY() WANTS() WANTED_BY()\n"
        "\t[3] RULE(ExplainRule3) NEEDS(ExplainRule1) NEEDED_BY(ExplainRule4) WANTS(ExplainRule2) WANTED_BY(ExplainRule5)\n"
        "\t[4] RULE(ExplainRule4) NEEDS(ExplainRule1) NEEDED_BY() WANTS(ExplainRule2) WANTED_BY()\n"
        "\t[5] RULE(ExplainRule5) NEEDS() NEEDED_BY() WANTS(ExplainRule4, 2 unregistered rules) WANTED_BY()\n"
        ")");
}


}
}
