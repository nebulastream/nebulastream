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


#include <any>
#include <optional>
#include <string>
#include <typeindex>
#include <unordered_map>
#include <utility>
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
    Reflected operator()(const InnerData& data) const
    {
        return reflect(detail::ReflectedInnerData{.value = data.value, .optionalField = data.optionalField});
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

///NOLINTBEGIN(bugprone-unchecked-optional-access
TEST(ReflectionTest, NestedContextPropagation)
{
    const OuterData outer{.name = "outer", .inner = InnerData{.value = "inner", .optionalField = 99}, .count = 5};

    auto reflected = reflect(outer);

    const ReflectionContext context{InnerDataDeserializationConfig{.includeOptionalField = true}};

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

    auto reflected = reflect(outer);

    const ReflectionContext context{InnerDataDeserializationConfig{.includeOptionalField = false}};

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

    auto reflected = reflect(vec);

    const ReflectionContext context{InnerDataDeserializationConfig{.includeOptionalField = true}};

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

    auto reflected = reflect(opt);

    const ReflectionContext context{InnerDataDeserializationConfig{.includeOptionalField = true}};

    auto deserialized = context.unreflect<std::optional<InnerData>>(reflected);

    ASSERT_TRUE(deserialized.has_value());
    EXPECT_EQ(deserialized->value, "test");
    EXPECT_EQ(deserialized->optionalField.value(), 42);
}

TEST(ReflectionTest, OptionalEmpty)
{
    const std::optional<InnerData> opt = std::nullopt;

    auto reflected = reflect(opt);

    const InnerDataDeserializationConfig config{.includeOptionalField = true};
    std::unordered_map<std::type_index, std::any> contextMap;
    contextMap[std::type_index(typeid(InnerData))] = config;
    const ReflectionContext context{contextMap};

    auto deserialized = context.unreflect<std::optional<InnerData>>(reflected);

    EXPECT_FALSE(deserialized.has_value());
}

///NOLINTEND(bugprone-unchecked-optional-access
}
