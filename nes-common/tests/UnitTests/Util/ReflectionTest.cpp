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


/// NOLINTBEGIN(readability-magic-numbers) Numeric values are fixture data in these tests.
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Util/Reflection.hpp>
#include <gtest/gtest.h>
#include <rfl/Generic.hpp>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
/// Inner type that uses context to determine deserialization behavior
struct InnerData
{
    std::string value;
    std::optional<int> optionalField;
};

/// Outer type that contains InnerData
struct OuterData
{
    std::string name;
    InnerData inner;
    int count;
};
}

namespace detail
{
namespace
{
/// We need the helper struct here even though the InnerData struct is reflectable,
/// so that we can overwrite the Unreflector for the InnerData without causing endless recursion
struct ReflectedInnerData
{
    std::string value;
    std::optional<int> optionalField;
};

}
}

namespace
{
/// Configuration struct to pass through context
struct InnerDataDeserializationConfig
{
    bool includeOptionalField;
};
}

template <>
struct Reflector<InnerData>
{
    Reflected operator()(const InnerData& data, const ReflectionContext& context) const
    {
        return context.reflect(detail::ReflectedInnerData{.value = data.value, .optionalField = data.optionalField});
    }
};

template <>
struct Unreflector<InnerData>
{
    using ContextType = InnerDataDeserializationConfig;

    ContextType config;

    explicit Unreflector(ContextType config) : config(std::move(config)) { }

    InnerData operator()(const Reflected& reflected, const ReflectionContext& context) const
    {
        auto [value, optionalField] = context.unreflect<detail::ReflectedInnerData>(reflected);

        InnerData result;
        result.value = value;

        /// Use context config to determine whether to include the optional field
        if (config.includeOptionalField)
        {
            result.optionalField = optionalField;
        }
        else
        {
            result.optionalField = std::nullopt;
        }

        return result;
    }
};

///NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST(ReflectionTest, NestedContextPropagation)
{
    const OuterData outer{.name = "outer", .inner = InnerData{.value = "inner", .optionalField = 99}, .count = 5};

    const ReflectionContext context{InnerDataDeserializationConfig{.includeOptionalField = true}};

    auto reflected = context.reflect(outer);

    auto deserialized = context.unreflect<OuterData>(reflected);

    EXPECT_EQ(deserialized.name, "outer");
    EXPECT_EQ(deserialized.inner.value, "inner");
    EXPECT_TRUE(deserialized.inner.optionalField.has_value());
    EXPECT_EQ(deserialized.inner.optionalField.value(), 99);
    EXPECT_EQ(deserialized.count, 5);
}

TEST(ReflectionTest, NestedContextPropagationExcludesOptionalField)
{
    const OuterData outer{.name = "outer", .inner = InnerData{.value = "inner", .optionalField = 99}, .count = 5};

    const ReflectionContext context{InnerDataDeserializationConfig{.includeOptionalField = false}};

    auto reflected = context.reflect(outer);

    auto deserialized = context.unreflect<OuterData>(reflected);

    EXPECT_EQ(deserialized.name, "outer");
    EXPECT_EQ(deserialized.inner.value, "inner");
    EXPECT_FALSE(deserialized.inner.optionalField.has_value());
    EXPECT_EQ(deserialized.count, 5);
}

TEST(ReflectionTest, VectorSerialization)
{
    const std::vector<InnerData> vec{InnerData{.value = "first", .optionalField = 1},
                                     InnerData{.value = "second", .optionalField = 2},
                                     InnerData{.value = "third", .optionalField = std::nullopt}};

    const ReflectionContext context{InnerDataDeserializationConfig{.includeOptionalField = true}};

    auto reflected = context.reflect(vec);

    auto deserialized = context.unreflect<std::vector<InnerData>>(reflected);

    ASSERT_EQ(deserialized.size(), 3);
    EXPECT_EQ(deserialized.at(0).value, "first");
    EXPECT_EQ(deserialized.at(0).optionalField.value(), 1);
    EXPECT_EQ(deserialized.at(1).value, "second");
    EXPECT_EQ(deserialized.at(1).optionalField.value(), 2);
    EXPECT_EQ(deserialized.at(2).value, "third");
    EXPECT_FALSE(deserialized.at(2).optionalField.has_value());
}

TEST(ReflectionTest, OptionalSerialization)
{
    const std::optional<InnerData> opt = InnerData{.value = "test", .optionalField = 42};

    const ReflectionContext context{InnerDataDeserializationConfig{.includeOptionalField = true}};

    auto reflected = context.reflect(opt);

    auto deserialized = context.unreflect<std::optional<InnerData>>(reflected);

    ASSERT_TRUE(deserialized.has_value());
    EXPECT_EQ(deserialized->value, "test");
    EXPECT_EQ(deserialized->optionalField.value(), 42);
}

TEST(ReflectionTest, OptionalEmpty)
{
    const std::optional<InnerData> opt = std::nullopt;

    const auto context = ReflectionContext{InnerDataDeserializationConfig{.includeOptionalField = true}};

    auto reflected = context.reflect(opt);

    auto deserialized = context.unreflect<std::optional<InnerData>>(reflected);

    EXPECT_FALSE(deserialized.has_value());
}

TEST(ReflectionTest, VariantSerialization)
{
    const std::variant<InnerData, int> variant = InnerData{.value = "test", .optionalField = 42};
    const ReflectionContext context{InnerDataDeserializationConfig{.includeOptionalField = true}};
    auto reflected = context.reflect(variant);
    auto deserialized = context.unreflect<std::variant<InnerData, int>>(reflected);
    ASSERT_TRUE(std::holds_alternative<InnerData>(deserialized));
    EXPECT_EQ(std::get<InnerData>(deserialized).value, "test");
    EXPECT_EQ(std::get<InnerData>(deserialized).optionalField.value(), 42);
}

TEST(ReflectionTest, PairSerialization)
{
    const std::pair<InnerData, int> pair = std::pair{InnerData{.value = "test", .optionalField = 42}, 123};
    const ReflectionContext context{InnerDataDeserializationConfig{.includeOptionalField = true}};
    auto reflected = context.reflect(pair);
    auto deserialized = context.unreflect<std::pair<InnerData, int>>(reflected);
    EXPECT_EQ(std::get<0>(deserialized).value, "test");
    EXPECT_EQ(std::get<0>(deserialized).optionalField.value(), 42);
    EXPECT_EQ(std::get<1>(deserialized), 123);
}

TEST(ReflectionTest, MapSerialization)
{
    const std::unordered_map<std::string, InnerData> map = {{"first", InnerData{.value = "first", .optionalField = 1}},
                                                            {"second", InnerData{.value = "second", .optionalField = 2}},
                                                            {"third", InnerData{.value = "third", .optionalField = std::nullopt}}};
    const ReflectionContext context{InnerDataDeserializationConfig{.includeOptionalField = true}};
    auto reflected = context.reflect(map);
    auto deserialized = context.unreflect<std::unordered_map<std::string, InnerData>>(reflected);
    ASSERT_EQ(deserialized.size(), 3);
    EXPECT_EQ(deserialized.at("first").value, "first");
    EXPECT_EQ(deserialized.at("first").optionalField.value(), 1);
    EXPECT_EQ(deserialized.at("second").value, "second");
    EXPECT_EQ(deserialized.at("second").optionalField.value(), 2);
    EXPECT_EQ(deserialized.at("third").value, "third");
    EXPECT_FALSE(deserialized.at("third").optionalField.has_value());
}

TEST(ReflectionTest, EnumSerialization)
{
    enum class TestEnum : uint8_t
    {
        First,
        Second,
        Third
    };
    const TestEnum testEnum = TestEnum::Second;
    const ReflectionContext context{InnerDataDeserializationConfig{.includeOptionalField = true}};
    auto reflected = context.reflect(testEnum);
    auto deserialized = context.unreflect<TestEnum>(reflected);
    EXPECT_EQ(deserialized, TestEnum::Second);
}

TEST(ReflectionTest, MissingConfig)
{
    const OuterData outer{.name = "outer", .inner = InnerData{.value = "inner", .optionalField = 99}, .count = 5};

    const ReflectionContext context{};
    auto reflected = context.reflect(outer);

    EXPECT_ANY_THROW(context.unreflect<OuterData>(reflected));
}

TEST(ReflectionTest, UnsignedConversion)
{
    /// Reflectcpp only has signed int64_t as a backing type in its rfl::Generic variant.
    /// C++ guarantees that for any uint64_t u static_cast<uint64_t>(static_cast<int64_t>(u)) == u ,
    /// but this test makes sure that no code of reflectcpp or us breaks this.
    constexpr uint64_t unsignedInput = std::dynamic_extent - 1;
    const ReflectionContext context{};
    const auto reflected = context.reflect(unsignedInput);
    const auto deserialized = context.unreflect<uint64_t>(reflected);
    EXPECT_EQ(deserialized, unsignedInput);
}

TEST(ReflectionTest, Unsigned32BitAboveIntMax)
{
    /// Regression: reflectcpp's Generic reader routes any integral with sizeof(T) <= sizeof(int)
    /// through a 32-bit, int-range-checked path. Since reflect() stores every integral as int64_t,
    /// a uint32_t above INT_MAX (a valid int64_t but out of `int` range) used to fail to unreflect.
    constexpr uint32_t unsignedInput = 0xFFFFFFFFU;
    static_assert(unsignedInput > static_cast<uint32_t>(std::numeric_limits<int>::max()));
    const ReflectionContext context{};
    const auto reflected = context.reflect(unsignedInput);
    const auto deserialized = context.unreflect<uint32_t>(reflected);
    EXPECT_EQ(deserialized, unsignedInput);
}

TEST(ReflectionTest, VariantWithUnsigned32BitAboveIntMax)
{
    /// Mirrors SourceDescriptor's config type: a variant whose active alternative is a large uint32_t.
    using ConfigType = std::variant<int32_t, uint32_t, int64_t, uint64_t>;
    const ConfigType input = static_cast<uint32_t>(3000000000U);
    const ReflectionContext context{};
    const auto reflected = context.reflect(input);
    const auto deserialized = context.unreflect<ConfigType>(reflected);
    ASSERT_TRUE(std::holds_alternative<uint32_t>(deserialized));
    EXPECT_EQ(std::get<uint32_t>(deserialized), 3000000000U);
}

namespace
{
/// Movable, non-default-constructible channel-like configuration.
struct MovableChannelConfig
{
    explicit MovableChannelConfig(size_t capacity) : capacity(capacity) { }

    MovableChannelConfig(const MovableChannelConfig&) = delete;
    MovableChannelConfig& operator=(const MovableChannelConfig&) = delete;
    MovableChannelConfig(MovableChannelConfig&&) = default;
    MovableChannelConfig& operator=(MovableChannelConfig&&) = delete;
    ~MovableChannelConfig() = default;

    size_t capacity;
};
}

template <>
struct Reflector<MovableChannelConfig>
{
    Reflected operator()(const MovableChannelConfig& value, const ReflectionContext& ctx) const { return ctx.reflect(value.capacity); }
};

template <>
struct Unreflector<MovableChannelConfig>
{
    MovableChannelConfig operator()(const Reflected& value, const ReflectionContext& ctx) const
    {
        return MovableChannelConfig{ctx.unreflect<size_t>(value)};
    }
};

namespace
{
struct SharedPointerCycle
{
    std::shared_ptr<SharedPointerCycle> next;
};
}

TEST(ReflectionTest, SharedPointerIdentityAcrossCopiesChildrenAndReversedPlans)
{
    const auto serializationState = std::make_shared<SharedPointerSerializationState>();
    const ReflectionContext writer{serializationState};
    const auto original = std::make_shared<MovableChannelConfig>(32);
    const auto first = writer.reflect(original);
    const auto& copy = writer;
    const auto second = copy.with(17).reflect(original);
    const auto distinct = writer.reflect(std::make_shared<MovableChannelConfig>(32));

    const auto deserializationState = std::make_shared<SharedPointerDeserializationState>();
    const ReflectionContext reader{deserializationState};
    const auto restoredSecond = reader.with(19).unreflect<std::shared_ptr<MovableChannelConfig>>(second);
    const auto restoredFirst = reader.unreflect<std::shared_ptr<MovableChannelConfig>>(first);
    EXPECT_EQ(restoredFirst, restoredSecond);
    EXPECT_NE(restoredFirst, original);
    EXPECT_EQ(restoredFirst->capacity, 32);
    EXPECT_NE(reader.unreflect<std::shared_ptr<MovableChannelConfig>>(distinct), restoredFirst);
    const auto fresh
        = ReflectionContext{std::make_shared<SharedPointerDeserializationState>()}.unreflect<std::shared_ptr<MovableChannelConfig>>(first);
    EXPECT_NE(fresh, restoredFirst);
    restoredFirst->capacity = 64;
    EXPECT_EQ(fresh->capacity, 32);
    const auto constView = reader.unreflect<std::shared_ptr<const MovableChannelConfig>>(first);
    EXPECT_EQ(constView, restoredFirst);
    EXPECT_EQ(rfl::json::write(*writer.reflect(std::shared_ptr<const MovableChannelConfig>{original})), rfl::json::write(*first));
}

TEST(ReflectionTest, SharedPointerConstructionCallbackRunsOnceForRepeatedDefinitions)
{
    const ReflectionContext writer{std::make_shared<SharedPointerSerializationState>()};
    const auto wire = writer.reflect(std::make_shared<int>(23));
    const auto envelope = ReflectionContext{}.unreflect<detail::SharedPointerDefinition>(wire);
    const auto fingerprint = rfl::json::write(*envelope.value);
    SharedPointerDeserializationState state;
    size_t constructions = 0;
    const auto first = state.getOrCreate<int>(envelope.id,
                                              fingerprint,
                                              [&constructions]
                                              {
                                                  ++constructions;
                                                  return std::make_shared<int>(23);
                                              });
    const auto second = state.getOrCreate<int>(envelope.id,
                                               fingerprint,
                                               [&constructions]
                                               {
                                                   ++constructions;
                                                   return std::make_shared<int>(99);
                                               });
    EXPECT_EQ(first, second);
    EXPECT_EQ(*second, 23);
    EXPECT_EQ(constructions, 1);
}

TEST(ReflectionTest, SharedPointerNullAndOwningCaches)
{
    const auto serializationState = std::make_shared<SharedPointerSerializationState>();
    const auto deserializationState = std::make_shared<SharedPointerDeserializationState>();
    const ReflectionContext context{serializationState, deserializationState};
    EXPECT_TRUE(context.reflect(std::shared_ptr<int>{}).isEmpty());
    EXPECT_EQ(context.unreflect<std::shared_ptr<int>>(Reflected{}), nullptr);
    auto original = std::make_shared<int>(42);
    const std::weak_ptr<int> serializedOwner = original;
    const auto wire = context.reflect(original);
    original.reset();
    EXPECT_FALSE(serializedOwner.expired());
    auto restored = context.unreflect<std::shared_ptr<int>>(wire);
    const std::weak_ptr<int> deserializedOwner = restored;
    restored.reset();
    EXPECT_FALSE(deserializedOwner.expired());
    EXPECT_EQ(context.unreflect<std::shared_ptr<int>>(wire), deserializedOwner.lock());
}

TEST(ReflectionTest, SharedPointerConflictingDefinitionsAndTypes)
{
    const ReflectionContext writer{std::make_shared<SharedPointerSerializationState>()};
    auto original = std::make_shared<int>(1);
    const auto wire = writer.reflect(original);
    *original = 2;
    EXPECT_THROW(static_cast<void>(writer.reflect(original)), Exception);

    /// Independent writers start at id 1, but different definitions must not be merged.
    const auto conflicting = ReflectionContext{std::make_shared<SharedPointerSerializationState>()}.reflect(std::make_shared<int>(2));
    const ReflectionContext reader{std::make_shared<SharedPointerDeserializationState>()};
    EXPECT_EQ(*reader.unreflect<std::shared_ptr<int>>(wire), 1);
    EXPECT_THROW(static_cast<void>(reader.unreflect<std::shared_ptr<int>>(conflicting)), Exception);
    EXPECT_THROW(static_cast<void>(reader.unreflect<std::shared_ptr<double>>(wire)), Exception);
    EXPECT_THROW(static_cast<void>(reader.unreflect<std::shared_ptr<int>>(reader.reflect(3))), Exception);

    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) same-address alias tests pointee-type rejection
    const auto otherType = std::shared_ptr<const unsigned int>(original, reinterpret_cast<const unsigned int*>(original.get()));
    EXPECT_THROW(static_cast<void>(writer.reflect(otherType)), Exception);
}

TEST(ReflectionTest, SharedPointerCyclesAreRejectedAndFailureDoesNotPoisonContext)
{
    const ReflectionContext context{std::make_shared<SharedPointerSerializationState>(),
                                    std::make_shared<SharedPointerDeserializationState>()};
    auto cycle = std::make_shared<SharedPointerCycle>();
    cycle->next = cycle;
    EXPECT_THROW(static_cast<void>(context.reflect(cycle)), Exception);
    cycle->next.reset();
    EXPECT_NO_THROW(static_cast<void>(context.reflect(cycle)));

    /// Finite malicious wire: nested occurrence reuses its parent's id.
    /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) key assignments construct the malformed wire object
    rfl::Generic::Object inner;
    inner["id"] = rfl::Generic{int64_t{1}};
    inner["value"] = rfl::Generic{std::nullopt};
    rfl::Generic::Object pointee;
    pointee["next"] = rfl::Generic{inner};
    rfl::Generic::Object outer;
    outer["id"] = rfl::Generic{int64_t{1}};
    outer["value"] = rfl::Generic{pointee};
    /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    EXPECT_THROW(static_cast<void>(context.unreflect<std::shared_ptr<SharedPointerCycle>>(Reflected{rfl::Generic{outer}})), Exception);
    EXPECT_NO_THROW(static_cast<void>(context.unreflect<std::shared_ptr<SharedPointerCycle>>(context.reflect(cycle))));
}

TEST(ReflectionTest, WithRetainsAddsAndOverridesTypedContextEntries)
{
    const ReflectionContext parent{InnerDataDeserializationConfig{true}};
    const auto input = parent.reflect(InnerData{.value = "channel", .optionalField = 7});
    const auto child = parent.with(123);
    EXPECT_EQ(ReflectionContext{}.with(InnerDataDeserializationConfig{true}).unreflect<InnerData>(input).optionalField, 7);
    EXPECT_EQ(child.unreflect<InnerData>(input).optionalField, 7);
    EXPECT_EQ(child.with().unreflect<InnerData>(input).optionalField, 7);
    EXPECT_FALSE(child.with(InnerDataDeserializationConfig{false}).unreflect<InnerData>(input).optionalField.has_value());
    EXPECT_EQ(parent.unreflect<InnerData>(input).optionalField, 7);
}

namespace
{
struct ValidatedSharedConfig
{
    bool senderClaimed = false;
    bool receiverClaimed = false;
    int validationCalls = 0;

    void validate()
    {
        ++validationCalls;
        if (!senderClaimed || !receiverClaimed)
        {
            throw CannotDeserialize("Both endpoints must be claimed");
        }
    }
};
}

TEST(ReflectionTest, SharedPointerContextRequiresTheCorrectInjectedState)
{
    const auto serializationState = std::make_shared<SharedPointerSerializationState>();
    const auto deserializationState = std::make_shared<SharedPointerDeserializationState>();
    const auto pointer = std::make_shared<int>(9);
    EXPECT_THROW(static_cast<void>(ReflectionContext{}.reflect(pointer)), Exception);
    EXPECT_THROW(static_cast<void>(ReflectionContext{}.reflect(std::shared_ptr<int>{})), Exception);
    EXPECT_THROW(static_cast<void>(ReflectionContext{}.unreflect<std::shared_ptr<int>>(Reflected{})), Exception);
    EXPECT_THROW(static_cast<void>(ReflectionContext{deserializationState}.reflect(pointer)), Exception);
    EXPECT_THROW(static_cast<void>(ReflectionContext{serializationState}.unreflect<std::shared_ptr<int>>(Reflected{})), Exception);
}

TEST(ReflectionTest, DeserializationEnumerationDoesNotInvokeDomainValidation)
{
    const ReflectionContext writer{std::make_shared<SharedPointerSerializationState>()};
    const auto source = std::make_shared<ValidatedSharedConfig>();
    const auto wire = writer.reflect(source);
    EXPECT_EQ(source->validationCalls, 0);

    std::weak_ptr<ValidatedSharedConfig> lifetime;
    auto deserializationState = std::make_shared<SharedPointerDeserializationState>();
    {
        const ReflectionContext reader{deserializationState};
        auto decoded = reader.unreflect<std::shared_ptr<ValidatedSharedConfig>>(wire);
        lifetime = decoded;
        const auto child = reader.with(42);
        EXPECT_EQ(child.unreflect<std::shared_ptr<ValidatedSharedConfig>>(wire), decoded);
        EXPECT_EQ(decoded->validationCalls, 0);
        const auto objects = deserializationState->getObjects<ValidatedSharedConfig>();
        ASSERT_EQ(objects.size(), 1);
        EXPECT_EQ(objects.front(), decoded);
        const auto constObjects = deserializationState->getObjects<const ValidatedSharedConfig>();
        ASSERT_EQ(constObjects.size(), 1);
        EXPECT_EQ(constObjects.front(), decoded);
        EXPECT_TRUE(deserializationState->getObjects<int>().empty());

        EXPECT_THROW(decoded->validate(), Exception);
        EXPECT_EQ(decoded->validationCalls, 1);
        decoded->senderClaimed = true;
        EXPECT_THROW(decoded->validate(), Exception);
        decoded->receiverClaimed = true;
        EXPECT_NO_THROW(decoded->validate());
        EXPECT_EQ(decoded->validationCalls, 3);
        decoded->receiverClaimed = false;
        EXPECT_THROW(decoded->validate(), Exception);
        EXPECT_EQ(decoded->validationCalls, 4);
        const auto plainWire = writer.reflect(std::make_shared<int>(7));
        EXPECT_EQ(*reader.unreflect<std::shared_ptr<int>>(plainWire), 7);
        EXPECT_EQ(deserializationState->getObjects<int>().size(), 1);
        decoded.reset();
        EXPECT_FALSE(lifetime.expired());
    }
    EXPECT_FALSE(lifetime.expired());
    EXPECT_EQ(source->validationCalls, 0);
    deserializationState.reset();
    EXPECT_TRUE(lifetime.expired());
    const auto freshState = std::make_shared<SharedPointerDeserializationState>();
    const ReflectionContext fresh{freshState};
    const auto freshDecoded = fresh.unreflect<std::shared_ptr<ValidatedSharedConfig>>(wire);
    EXPECT_NE(freshDecoded, lifetime.lock());
    EXPECT_EQ(freshDecoded->validationCalls, 0);
    EXPECT_EQ(freshState->getObjects<ValidatedSharedConfig>().size(), 1);
}

///NOLINTEND(bugprone-unchecked-optional-access)
}

/// NOLINTEND(readability-magic-numbers)
