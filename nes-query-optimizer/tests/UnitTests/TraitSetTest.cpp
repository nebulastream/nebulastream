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

#include <cstdint>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Traits/ImplementationTypeTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>

namespace NES
{
namespace
{

class TraitSetTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TraitSetTest.log", LogLevel::LOG_DEBUG); }
};

/// A default-constructed TraitSet should be empty.
TEST_F(TraitSetTest, DefaultConstructedIsEmpty)
{
    const TraitSet traitSet;
    EXPECT_EQ(traitSet.size(), 0);
    EXPECT_FALSE(traitSet.contains<JoinImplementationTypeTrait>());
    EXPECT_FALSE(traitSet.contains<MemoryLayoutTypeTrait>());
}

/// Constructing a TraitSet with variadic traits stores all of them.
TEST_F(TraitSetTest, VariadicConstructor)
{
    const TraitSet traitSet(
        JoinImplementationTypeTrait{JoinImplementation::HASH_JOIN}, MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});
    EXPECT_EQ(traitSet.size(), 2);
    EXPECT_TRUE(traitSet.contains<JoinImplementationTypeTrait>());
    EXPECT_TRUE(traitSet.contains<MemoryLayoutTypeTrait>());
}

/// tryInsert should succeed on first call and fail on duplicate.
TEST_F(TraitSetTest, TryInsertSucceedsOnceAndRejectsDuplicate)
{
    TraitSet traitSet;
    const bool first = traitSet.tryInsert(JoinImplementationTypeTrait{JoinImplementation::HASH_JOIN});
    EXPECT_TRUE(first);
    EXPECT_EQ(traitSet.size(), 1);

    /// Second insert of the same trait type should fail.
    const bool second = traitSet.tryInsert(JoinImplementationTypeTrait{JoinImplementation::NESTED_LOOP_JOIN});
    EXPECT_FALSE(second);
    EXPECT_EQ(traitSet.size(), 1);

    /// The original value should be preserved (not overwritten).
    auto trait = traitSet.get<JoinImplementationTypeTrait>();
    EXPECT_EQ(trait->implementationType, JoinImplementation::HASH_JOIN);
}

/// get<T> returns the correct trait value.
TEST_F(TraitSetTest, GetReturnsCorrectValue)
{
    TraitSet traitSet;
    (void)traitSet.tryInsert(MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});

    auto trait = traitSet.get<MemoryLayoutTypeTrait>();
    EXPECT_EQ(trait->memoryLayout, MemoryLayoutType::ROW_LAYOUT);
}

/// tryGet<T> returns nullopt when the trait is absent.
TEST_F(TraitSetTest, TryGetReturnsNulloptWhenAbsent)
{
    const TraitSet traitSet;
    auto result = traitSet.tryGet<JoinImplementationTypeTrait>();
    EXPECT_FALSE(result.has_value());
}

/// tryGet<T> returns the trait when present.
TEST_F(TraitSetTest, TryGetReturnsValueWhenPresent)
{
    TraitSet traitSet;
    (void)traitSet.tryInsert(JoinImplementationTypeTrait{JoinImplementation::NESTED_LOOP_JOIN});

    auto result = traitSet.tryGet<JoinImplementationTypeTrait>();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value()->implementationType, JoinImplementation::NESTED_LOOP_JOIN);
}

/// Two TraitSets with the same traits should be equal.
TEST_F(TraitSetTest, EqualityWithSameTraits)
{
    TraitSet a;
    (void)a.tryInsert(JoinImplementationTypeTrait{JoinImplementation::HASH_JOIN});
    (void)a.tryInsert(MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});

    TraitSet b;
    (void)b.tryInsert(JoinImplementationTypeTrait{JoinImplementation::HASH_JOIN});
    (void)b.tryInsert(MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});

    EXPECT_EQ(a, b);
}

/// Two TraitSets with different trait values should not be equal.
TEST_F(TraitSetTest, InequalityWithDifferentValues)
{
    TraitSet a;
    (void)a.tryInsert(JoinImplementationTypeTrait{JoinImplementation::HASH_JOIN});

    TraitSet b;
    (void)b.tryInsert(JoinImplementationTypeTrait{JoinImplementation::NESTED_LOOP_JOIN});

    EXPECT_NE(a, b);
}

/// Free-standing helper functions hasTrait / hasTraits / getTrait work correctly.
TEST_F(TraitSetTest, FreeStandingHelpers)
{
    TraitSet traitSet;
    (void)traitSet.tryInsert(JoinImplementationTypeTrait{JoinImplementation::CHOICELESS});
    (void)traitSet.tryInsert(MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});

    EXPECT_TRUE(hasTrait<JoinImplementationTypeTrait>(traitSet));
    EXPECT_TRUE(hasTrait<MemoryLayoutTypeTrait>(traitSet));
    EXPECT_TRUE((hasTraits<JoinImplementationTypeTrait, MemoryLayoutTypeTrait>(traitSet)));
    EXPECT_FALSE(hasTrait<OutputOriginIdsTrait>(traitSet));

    auto got = getTrait<JoinImplementationTypeTrait>(traitSet);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got.value()->implementationType, JoinImplementation::CHOICELESS);
}

/// Constructing from a range of Trait (type-erased) values.
TEST_F(TraitSetTest, RangeConstructor)
{
    std::vector<Trait> traits;
    traits.emplace_back(JoinImplementationTypeTrait{JoinImplementation::HASH_JOIN});
    traits.emplace_back(MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});

    const TraitSet traitSet(traits);
    EXPECT_EQ(traitSet.size(), 2);
    EXPECT_TRUE(traitSet.contains<JoinImplementationTypeTrait>());
    EXPECT_TRUE(traitSet.contains<MemoryLayoutTypeTrait>());
}

/// The TraitSet is iterable.
TEST_F(TraitSetTest, IsIterable)
{
    TraitSet traitSet;
    (void)traitSet.tryInsert(JoinImplementationTypeTrait{JoinImplementation::HASH_JOIN});
    (void)traitSet.tryInsert(MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});

    size_t count = 0;
    for ([[maybe_unused]] const auto& [typeIndex, trait] : traitSet)
    {
        ++count;
    }
    EXPECT_EQ(count, 2);
}

/// OutputOriginIdsTrait stores and exposes origin IDs correctly.
TEST_F(TraitSetTest, OutputOriginIdsTraitRoundTrip)
{
    std::vector<OriginId> ids = {OriginId(1), OriginId(2), OriginId(3)};
    TraitSet traitSet;
    (void)traitSet.tryInsert(OutputOriginIdsTrait{ids});

    ASSERT_TRUE(traitSet.contains<OutputOriginIdsTrait>());
    auto trait = traitSet.get<OutputOriginIdsTrait>();
    EXPECT_EQ(trait->size(), 3);
    EXPECT_EQ((*trait)[0], OriginId(1));
    EXPECT_EQ((*trait)[1], OriginId(2));
    EXPECT_EQ((*trait)[2], OriginId(3));
}

}
}
