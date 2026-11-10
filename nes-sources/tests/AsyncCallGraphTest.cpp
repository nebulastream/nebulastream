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
#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <DataTypes/RuntimeValue.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Util/Reflection.hpp>
#include <gtest/gtest.h>
#include <rfl/Generic.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
const DataType INTEGER{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
const DataType UNSIGNED{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};

AsyncCallGraph diamond()
{
    AsyncCallGraphBuilder builder{{}};
    const auto producer = builder.call(AsyncFunctionType{"Producer"}, {RuntimeValue{int64_t{42}}}, {INTEGER, UNSIGNED});
    const auto left = builder.call(AsyncFunctionType{"Left"}, {producer->output(0)}, {INTEGER});
    const auto right = builder.call(AsyncFunctionType{"Right"}, {producer->output(1)}, {UNSIGNED});
    builder.exportOutput(left->output(0), QualifiedIdentifier::parse("LEFT"));
    builder.exportOutput(right->output(0), QualifiedIdentifier::parse("RIGHT"));
    return builder.build();
}

AsyncCallGraph decode(const rfl::Generic& wire)
{
    const auto parsed = rfl::json::read<rfl::Generic>(rfl::json::write(wire));
    if (!parsed)
    {
        throw CannotDeserialize(parsed.error().what());
    }
    return ReflectionContext{}.unreflect<AsyncCallGraph>(*parsed);
}

rfl::Generic::Array& calls(rfl::Generic& wire)
{
    return std::get<rfl::Generic::Array>(std::get<rfl::Generic::Object>(wire.variant())["calls"].variant());
}

rfl::Generic::Object& object(rfl::Generic& value)
{
    return std::get<rfl::Generic::Object>(value.variant());
}

rfl::Generic::Object& firstReference(rfl::Generic& call)
{
    auto& arguments = std::get<rfl::Generic::Array>(object(call)["arguments"].variant());
    return object(object(arguments.front())["value"]);
}
}

static_assert(!std::is_default_constructible_v<AsyncResultReference>);
static_assert(!std::is_default_constructible_v<InputFieldReference>);

TEST(AsyncCallGraphTest, OutputHandleOwnsItsProducerAndChecksItsSelector)
{
    std::weak_ptr<const AsyncCall> weak;
    auto output = [&]
    {
        AsyncCallGraphBuilder builder{{}};
        const auto producer = builder.call(AsyncFunctionType{"Producer"}, {}, {INTEGER});
        weak = producer;
        EXPECT_THROW(static_cast<void>(producer->output(1)), Exception);
        return producer->output(0);
    }();
    /// The builder and every other owning handle have gone away.
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(output.getDataType(), INTEGER);
    EXPECT_EQ(output.getProducer(), weak.lock());
    const auto copy = std::move(output);
    EXPECT_EQ(copy.getProducer(), output.getProducer());
}

TEST(AsyncCallGraphTest, RejectsForeignProducerAndInputEvenWhenNamesAndTypesAgree)
{
    const Schema<QualifiedUnboundField, Ordered> schema{QualifiedUnboundField{QualifiedIdentifier::parse("SOURCE.VALUE"), INTEGER}};
    AsyncCallGraphBuilder first{schema};
    AsyncCallGraphBuilder second{schema};
    const auto field = first.input(QualifiedIdentifier::parse("SOURCE.VALUE"));
    EXPECT_EQ(field.getField(), QualifiedIdentifier::parse("SOURCE.VALUE"));
    EXPECT_EQ(field.getDataType(), INTEGER);
    const auto producer = first.call(AsyncFunctionType{"Producer"}, {field}, {INTEGER});
    EXPECT_THROW(static_cast<void>(second.call(AsyncFunctionType{"Bad"}, {field}, {INTEGER})), Exception);
    EXPECT_THROW(static_cast<void>(second.call(AsyncFunctionType{"Bad"}, {producer->output(0)}, {INTEGER})), Exception);
    EXPECT_THROW(second.exportOutput(producer->output(0), QualifiedIdentifier::parse("OUT")), Exception);
    EXPECT_THROW(static_cast<void>(first.input(QualifiedIdentifier::parse("VALUE"))), Exception);
}

TEST(AsyncCallGraphTest, ReflectionPreservesFullyQualifiedCaseSensitiveInputs)
{
    const auto name = QualifiedIdentifier::parse("\"Input\".\"Value\"");
    const Schema<QualifiedUnboundField, Ordered> schema{QualifiedUnboundField{name, INTEGER}};
    AsyncCallGraphBuilder builder{schema};
    const auto producer = builder.call(AsyncFunctionType{"Identity"}, {builder.input(name)}, {INTEGER});
    builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("OUT"));
    const auto copy = decode(*ReflectionContext{}.reflect(builder.build()));
    EXPECT_EQ(copy.getInputSchema(), schema);
    const auto& input = std::get<InputFieldReference>(copy.getCalls().front()->getArguments().front());
    EXPECT_EQ(input.getField(), name);
    EXPECT_EQ(input.getDataType(), INTEGER);
}

TEST(AsyncCallGraphTest, BuilderIsSingleUseAndPrunesUnreachableWork)
{
    AsyncCallGraphBuilder builder{{}};
    const auto unused = builder.call(AsyncFunctionType{"Unused"}, {}, {INTEGER});
    const auto used = builder.call(AsyncFunctionType{"Used"}, {}, {INTEGER});
    builder.exportOutput(used->output(0), QualifiedIdentifier::parse("OUT"));
    auto graph = builder.build();
    ASSERT_EQ(graph.getCalls().size(), 1);
    EXPECT_EQ(graph.getCalls().front(), used);
    EXPECT_THROW(static_cast<void>(builder.call(AsyncFunctionType{"Late"}, {}, {INTEGER})), Exception);
    EXPECT_THROW(static_cast<void>(builder.build()), Exception);
}

TEST(AsyncCallGraphTest, ReflectionPreservesDiamondIdentityWithoutDependingOnDefinitionOrder)
{
    const auto original = diamond();
    auto wire = *ReflectionContext{}.reflect(original);
    EXPECT_TRUE(object(calls(wire).front()).get("functionType").has_value());
    EXPECT_FALSE(object(calls(wire).front()).get("function").has_value());
    std::ranges::reverse(calls(wire));
    const auto copy = decode(wire);
    ASSERT_EQ(copy.getCalls().size(), 3);
    ASSERT_EQ(copy.getOutputs().size(), 2);
    const auto left = copy.getOutputs()[0].result.getProducer();
    const auto right = copy.getOutputs()[1].result.getProducer();
    EXPECT_EQ(left->getFunctionType(), AsyncFunctionType{"Left"});
    EXPECT_EQ(right->getFunctionType(), AsyncFunctionType{"Right"});
    const auto& leftInput = std::get<AsyncResultReference>(left->getArguments().front());
    const auto& rightInput = std::get<AsyncResultReference>(right->getArguments().front());
    EXPECT_EQ(leftInput.getProducer(), rightInput.getProducer());
    EXPECT_EQ(leftInput.getDataType(), INTEGER);
    EXPECT_EQ(rightInput.getDataType(), UNSIGNED);
    EXPECT_NE(leftInput.getProducer(), original.getCalls().front());
}

TEST(AsyncCallGraphTest, ReflectionRejectsDanglingDuplicateCyclicAndOutOfRangeReferences)
{
    const auto graph = diamond();
    const ReflectionContext context;
    auto dangling = *context.reflect(graph);
    firstReference(calls(dangling)[1])["producer"] = std::string{"missing"};
    EXPECT_THROW(static_cast<void>(decode(dangling)), Exception);
    auto duplicate = *context.reflect(graph);
    object(calls(duplicate)[1])["id"] = object(calls(duplicate)[0])["id"];
    EXPECT_THROW(static_cast<void>(decode(duplicate)), Exception);
    auto invalidOutput = *context.reflect(graph);
    firstReference(calls(invalidOutput)[1])["output"] = int64_t{99};
    EXPECT_THROW(static_cast<void>(decode(invalidOutput)), Exception);
    auto cycle = *context.reflect(graph);
    /// Make Producer read Left, while Left already reads Producer. Variant alternative 2 is a producer handle.
    auto reference = std::get<rfl::Generic::Array>(object(calls(cycle)[1])["arguments"].variant()).front();
    object(object(reference)["value"])["producer"] = std::string{"node-1"};
    object(calls(cycle)[0])["arguments"] = rfl::Generic::Array{std::move(reference)};
    EXPECT_THROW(static_cast<void>(decode(cycle)), Exception);
    auto badType = *context.reflect(graph);
    auto& types = std::get<rfl::Generic::Array>(object(calls(badType)[0])["resultTypes"].variant());
    types[0] = rfl::Generic::Array{int64_t{7}};
    EXPECT_THROW(static_cast<void>(decode(badType)), Exception);
}

TEST(AsyncCallGraphTest, NativeConstantsRoundTripAllTypesNullabilityAndExactPayloads)
{
    constexpr std::array types{
        DataType::Type::BOOLEAN,
        DataType::Type::CHAR,
        DataType::Type::UINT8,
        DataType::Type::UINT16,
        DataType::Type::UINT32,
        DataType::Type::UINT64,
        DataType::Type::INT8,
        DataType::Type::INT16,
        DataType::Type::INT32,
        DataType::Type::INT64,
        DataType::Type::FLOAT32,
        DataType::Type::FLOAT64,
        DataType::Type::VARSIZED};
    for (const auto type : types)
    {
        for (const auto nullable : {DataType::NULLABLE::NOT_NULLABLE, DataType::NULLABLE::IS_NULLABLE})
        {
            const DataType dataType{type, nullable};
            visitRuntimeType(
                type,
                [&]<typename T>()
                {
                    const RuntimeValue value = [&]() -> RuntimeValue
                    {
                        if constexpr (std::is_same_v<T, std::string_view>)
                        {
                            return RuntimeValue{dataType, std::string{"a\0b", 3}};
                        }
                        else
                        {
                            return RuntimeValue{dataType, std::numeric_limits<T>::max()};
                        }
                    }();
                    AsyncCallGraphBuilder builder{{}};
                    auto producer = builder.call(AsyncFunctionType{"Constant"}, {value}, {dataType});
                    builder.exportOutput(producer->output(0), QualifiedIdentifier::parse("OUT"));
                    const auto graph = decode(*ReflectionContext{}.reflect(builder.build()));
                    const auto& result = std::get<RuntimeValue>(graph.getCalls().front()->getArguments().front());
                    EXPECT_EQ(result.getDataType(), dataType);
                    EXPECT_EQ(result.view().get<T>(), value.view().get<T>());
                });
        }
        const DataType nullableType{type, DataType::NULLABLE::IS_NULLABLE};
        const auto value = RuntimeValue::null(nullableType);
        const auto copy = ReflectionContext{}.unreflect<RuntimeValue>(ReflectionContext{}.reflect(value));
        EXPECT_TRUE(copy.isNull());
        EXPECT_EQ(copy.getDataType(), nullableType);
    }
}

TEST(AsyncCallGraphTest, VariableSizedReflectionPreservesArbitraryBytesNotOnlyUtf8)
{
    std::string bytes;
    for (unsigned int value = 0; value < 256; ++value)
    {
        bytes.push_back(std::bit_cast<char>(static_cast<uint8_t>(value)));
    }
    const RuntimeValue value{bytes};
    const ReflectionContext context;
    const auto parsed = rfl::json::read<rfl::Generic>(rfl::json::write(*context.reflect(value)));
    ASSERT_TRUE(parsed);
    const auto copy = context.unreflect<RuntimeValue>(*parsed);
    EXPECT_EQ(copy.view().get<std::string_view>(), bytes);
}

TEST(AsyncCallGraphTest, FloatingPointReflectionPreservesNanPayloadAndSignedZero)
{
    const ReflectionContext context;
    for (const auto bits : {uint64_t{0x8000000000000000}, uint64_t{0x7ff8000000000042}, uint64_t{0x7ff0000000000000}})
    {
        const RuntimeValue value{std::bit_cast<double>(bits)};
        const auto parsed = rfl::json::read<rfl::Generic>(rfl::json::write(*context.reflect(value)));
        ASSERT_TRUE(parsed);
        const auto copy = context.unreflect<RuntimeValue>(*parsed);
        EXPECT_EQ(std::bit_cast<uint64_t>(copy.view().get<double>()), bits);
    }
}

}
