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
#include <optional>
#include <span>
#include <string>
#include <typeindex>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Util/Reflection.hpp>
#include <gtest/gtest.h>

namespace NES
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

namespace detail
{
/// We need the helper struct here even though the InnerData struct is reflectable,
/// so that we can overwrite the Unreflector for the InnerData without causing endless recursion
struct ReflectedInnerData
{
    std::string value;
    std::optional<int> optionalField;
};

}

/// Configuration struct to pass through context
struct InnerDataDeserializationConfig
{
    bool includeOptionalField;
};

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
    const std::vector<InnerData> vec{
        InnerData{.value = "first", .optionalField = 1},
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
    const std::unordered_map<std::string, InnerData> map
        = {{"first", InnerData{.value = "first", .optionalField = 1}},
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

///NOLINTEND(bugprone-unchecked-optional-access)
}
