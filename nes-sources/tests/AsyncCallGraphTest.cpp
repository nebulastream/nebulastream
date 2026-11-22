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
#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <Async/HandoffChannel.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncSourceConfig.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp> /// NOLINT(misc-include-cleaner) Public Logger facade declares setupLogging; checker misattributes it.
#include <Util/Reflection.hpp>
#include <Util/Reflection/SharedPointerDeserializationState.hpp>
#include <Util/Reflection/SharedPointerSerializationState.hpp>
#include <gtest/gtest.h>
#include <rfl/Generic.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <AsyncTestOperator.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
const DataType INTEGER{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
const DataType UNSIGNED{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};

AsyncCall::Ptr
makeCall(AsyncCallGraphBuilder& builder, std::string label, std::vector<AsyncCallArgument> arguments, std::vector<DataType> resultTypes)
{
    return builder.call(makeAsyncTestOperator(std::move(label), std::move(resultTypes)), std::move(arguments));
}

AsyncCallGraph diamond()
{
    AsyncCallGraphBuilder builder{{}};
    const auto producer = makeCall(builder, "Producer", {RuntimeValue{int64_t{42}}}, {INTEGER, UNSIGNED});
    const auto left = makeCall(builder, "Left", {producer->output(0)}, {INTEGER});
    const auto right = makeCall(builder, "Right", {producer->output(1)}, {UNSIGNED});
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

rfl::Generic::Object& nativeOperator(rfl::Generic& call)
{
    return object(object(call)["asyncOperator"]);
}

rfl::Generic::Object& nativeConfig(rfl::Generic& call)
{
    return object(nativeOperator(call)["config"]);
}

rfl::Generic::Object& firstReference(rfl::Generic& call)
{
    auto& arguments = std::get<rfl::Generic::Array>(object(call)["arguments"].variant());
    return object(object(arguments.front())["value"]);
}
}

static_assert(!std::is_default_constructible_v<AsyncResultReference>);
static_assert(!std::is_default_constructible_v<InputFieldReference>);

TEST(AsyncCallGraphTest, SourceConfigKeepsTheTypedGraphAndReflectsItAsAnObject)
{
    static_assert(std::is_same_v<decltype(AsyncSourceConfig::calls), AsyncCallGraph>);
    static_assert(std::is_same_v<decltype(AsyncSourceConfig::channel), std::shared_ptr<HandoffChannel>>);
    const AsyncSourceConfig config{
        .channel = std::make_shared<HandoffChannel>(2), .calls = diamond(), .maxConcurrency = 3, .maxRetries = 1};
    const auto& copy = config;
    EXPECT_EQ(copy.calls.getCalls().front(), config.calls.getCalls().front());
    const ReflectionContext writer{std::make_shared<SharedPointerSerializationState>()};
    const auto reflected = writer.reflect(config);
    auto wire = *reflected;
    EXPECT_TRUE(std::holds_alternative<rfl::Generic::Object>(object(wire).at("calls").variant()));
    const ReflectionContext reader{std::make_shared<SharedPointerDeserializationState>()};
    const auto restored = reader.unreflect<AsyncSourceConfig>(reflected);
    EXPECT_NE(restored.channel, config.channel);
    EXPECT_EQ(copy.channel, config.channel);
    EXPECT_EQ(restored.maxConcurrency, 3);
    EXPECT_EQ(restored.channel->getCapacity(), 2);
    EXPECT_EQ(restored.maxRetries, 1);
    ASSERT_EQ(restored.calls.getCalls().size(), 3);
    ASSERT_EQ(restored.calls.getOutputs().size(), 2);
    const auto left = restored.calls.getOutputs()[0].result.getProducer();
    const auto right = restored.calls.getOutputs()[1].result.getProducer();
    const auto leftInput = std::get<AsyncResultReference>(left->getArguments()[0]);
    const auto rightInput = std::get<AsyncResultReference>(right->getArguments()[0]);
    EXPECT_EQ(leftInput.getProducer(), rightInput.getProducer());
    EXPECT_EQ(leftInput.getDataType(), INTEGER);
    EXPECT_EQ(rightInput.getDataType(), UNSIGNED);
}

TEST(AsyncCallGraphTest, OwnsNativeConfigAndPreservesCustomStateThroughReflection)
{
    auto config = makeAsyncTestOperator("StatefulTest", {INTEGER}, 918);
    AsyncCallGraphBuilder builder{{}};
    const auto call = builder.call(config, {RuntimeValue{int64_t{42}}});
    builder.exportOutput(call->output(0), QualifiedIdentifier::parse("RESULT"));
    const auto graph = builder.build();
    EXPECT_EQ(graph.getCalls().front()->getAsyncOperator().getType(), "StatefulTest");
    EXPECT_EQ(graph.getCalls().front()->getResultTypes(), std::vector<DataType>{INTEGER});
    const auto reflected = ReflectionContext{}.reflect(graph);
    const auto copy = ReflectionContext{}.unreflect<AsyncCallGraph>(reflected);
    ASSERT_EQ(copy.getCalls().size(), 1);
    const auto restored = copy.getCalls().front()->getAsyncOperator().getAs<AsyncTestOperatorConfig>();
    EXPECT_EQ(restored->marker, 918);
    EXPECT_EQ(restored->label, "StatefulTest");
}

TEST(AsyncCallGraphTest, PreservesSharedProducerIdentityAndNamedOutputs)
{
    AsyncCallGraphBuilder builder{{}};
    const auto producer = builder.call(makeAsyncTestOperator("SharedProducer", {INTEGER, INTEGER}), {});
    const auto first = builder.call(makeAsyncTestOperator("FirstConsumer", {INTEGER}), {producer->output(0)});
    const auto second = builder.call(makeAsyncTestOperator("SecondConsumer", {INTEGER}), {producer->output(1)});
    builder.exportOutput(first->output(0), QualifiedIdentifier::parse("FIRST"));
    builder.exportOutput(second->output(0), QualifiedIdentifier::parse("SECOND"));
    const auto graph = builder.build();
    const auto restored = ReflectionContext{}.unreflect<AsyncCallGraph>(ReflectionContext{}.reflect(graph));
    ASSERT_EQ(restored.getCalls().size(), 3);
    const auto& firstInput = std::get<AsyncResultReference>(restored.getCalls()[1]->getArguments()[0]);
    const auto& secondInput = std::get<AsyncResultReference>(restored.getCalls()[2]->getArguments()[0]);
    EXPECT_EQ(firstInput.getProducer(), secondInput.getProducer());
    EXPECT_EQ(firstInput.getOutputIndex(), 0);
    EXPECT_EQ(secondInput.getOutputIndex(), 1);
    ASSERT_EQ(restored.getOutputs().size(), 2);
    EXPECT_EQ(restored.getOutputs()[0].field, QualifiedIdentifier::parse("FIRST"));
    EXPECT_EQ(restored.getOutputs()[1].field, QualifiedIdentifier::parse("SECOND"));
}

TEST(AsyncCallGraphTest, CallOutputsAreDerivedFromNativeOutputSchema)
{
    AsyncCallGraphBuilder builder{{}};
    const auto call = builder.call(makeAsyncTestOperator("TwoOutputs", {INTEGER, INTEGER}), {});
    EXPECT_EQ(call->getResultTypes().size(), 2);
    EXPECT_EQ(call->output(1).getDataType(), INTEGER);
}

/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(AsyncCallGraphTest, OutputHandleOwnsItsProducerAndChecksItsSelector)
{
#ifndef NO_ASSERT
    SKIP_IF_TSAN();
#endif
    std::weak_ptr<const AsyncCall> weak;
    auto output = [&]
    {
        AsyncCallGraphBuilder builder{{}};
        const auto producer = makeCall(builder, "Producer", {}, {INTEGER});
        weak = producer;
#ifndef NO_ASSERT
        Logger::setupLogging(
            "AsyncCallGraphDeathTest.log",
            LogLevel::LOG_DEBUG,
            false);
        GTEST_FLAG_SET(death_test_style, "threadsafe");
        EXPECT_DEATH(static_cast<void>(producer->output(1)), "Precondition violated");
#endif
        return producer->output(0);
    }();
    /// The builder and every other owning handle have gone away.
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(output.getDataType(), INTEGER);
    EXPECT_EQ(output.getProducer(), weak.lock());
}
/// NOLINTEND(misc-include-cleaner)

TEST(AsyncCallGraphTest, RejectsForeignProducerAndInputEvenWhenNamesAndTypesAgree)
{
    const Schema<QualifiedUnboundField, Ordered> schema{QualifiedUnboundField{QualifiedIdentifier::parse("SOURCE.VALUE"), INTEGER}};
    AsyncCallGraphBuilder first{schema};
    AsyncCallGraphBuilder second{schema};
    const auto field = first.input(QualifiedIdentifier::parse("SOURCE.VALUE"));
    EXPECT_EQ(field.getField(), QualifiedIdentifier::parse("SOURCE.VALUE"));
    EXPECT_EQ(field.getDataType(), INTEGER);
    const auto producer = makeCall(first, "Producer", {field}, {INTEGER});
#ifndef NO_ASSERT
    SKIP_IF_TSAN();
    Logger::setupLogging("AsyncCallGraphDeathTest.log", LogLevel::LOG_DEBUG, false);
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(static_cast<void>(makeCall(second, "Bad", {field}, {INTEGER})), "Precondition violated");
    EXPECT_DEATH(static_cast<void>(makeCall(second, "Bad", {producer->output(0)}, {INTEGER})), "Precondition violated");
    EXPECT_DEATH(second.exportOutput(producer->output(0), QualifiedIdentifier::parse("OUT")), "Precondition violated");
    EXPECT_DEATH(static_cast<void>(first.input(QualifiedIdentifier::parse("VALUE"))), "Precondition violated");
#endif
}

#ifndef NO_ASSERT
TEST(AsyncCallGraphTest, BuilderContractMisuseDies)
{
    SKIP_IF_TSAN();
    Logger::setupLogging("AsyncCallGraphDeathTest.log", LogLevel::LOG_DEBUG, false);
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        (
            []
            {
                const AsyncCallGraphBuilder builder{Schema<QualifiedUnboundField, Ordered>{
                    QualifiedUnboundField{QualifiedIdentifier::parse("DUP"), INTEGER},
                    QualifiedUnboundField{QualifiedIdentifier::parse("DUP"), INTEGER}}};
                static_cast<void>(builder);
            }()),
        "Precondition violated.*Duplicate async input");

    EXPECT_DEATH(
        {
            AsyncCallGraphBuilder builder{{}};
            const auto call = makeCall(builder, "Producer", {}, {INTEGER});
            builder.exportOutput(call->output(0), QualifiedIdentifier::parse("OUT"));
            builder.exportOutput(call->output(0), QualifiedIdentifier::parse("OUT"));
        },
        "Precondition violated.*Duplicate async output");
    EXPECT_DEATH(
        {
            AsyncCallGraphBuilder builder{{}};
            static_cast<void>(makeCall(builder, "", {}, {INTEGER}));
        },
        "Precondition violated.*needs a function");
    EXPECT_DEATH(
        {
            AsyncCallGraphBuilder builder{{}};
            static_cast<void>(makeCall(builder, "NoResult", {}, {}));
        },
        "Precondition violated.*needs a function");
}
#endif

TEST(AsyncCallGraphTest, ReflectionPreservesFullyQualifiedCaseSensitiveInputs)
{
    const auto name = QualifiedIdentifier::parse(R"("Input"."Value")");
    const Schema<QualifiedUnboundField, Ordered> schema{QualifiedUnboundField{name, INTEGER}};
    AsyncCallGraphBuilder builder{schema};
    const auto producer = makeCall(builder, "Identity", {builder.input(name)}, {INTEGER});
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
    const auto unused = makeCall(builder, "Unused", {}, {INTEGER});
    const auto used = makeCall(builder, "Used", {}, {INTEGER});
    builder.exportOutput(used->output(0), QualifiedIdentifier::parse("OUT"));
    auto graph = builder.build();
    ASSERT_EQ(graph.getCalls().size(), 1);
    EXPECT_EQ(graph.getCalls().front(), used);
#ifndef NO_ASSERT
    SKIP_IF_TSAN();
    Logger::setupLogging("AsyncCallGraphDeathTest.log", LogLevel::LOG_DEBUG, false);
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(static_cast<void>(makeCall(builder, "Late", {}, {INTEGER})), "Precondition violated");
    EXPECT_DEATH(static_cast<void>(builder.build()), "Precondition violated");
#endif
}

TEST(AsyncCallGraphTest, ReflectionPreservesDiamondIdentityWithoutDependingOnDefinitionOrder)
{
    const auto original = diamond();
    auto wire = *ReflectionContext{}.reflect(original);
    EXPECT_TRUE(nativeOperator(calls(wire).front()).get("type").has_value());
    EXPECT_FALSE(object(calls(wire).front()).get("functionType").has_value());
    std::ranges::reverse(calls(wire));
    const auto copy = decode(wire);
    ASSERT_EQ(copy.getCalls().size(), 3);
    ASSERT_EQ(copy.getOutputs().size(), 2);
    const auto left = copy.getOutputs()[0].result.getProducer();
    const auto right = copy.getOutputs()[1].result.getProducer();
    EXPECT_EQ(left->getFunctionType(), "Left");
    EXPECT_EQ(right->getFunctionType(), "Right");
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
    auto& types = std::get<rfl::Generic::Array>(nativeConfig(calls(badType)[0])["resultTypes"].variant());
    types[0] = rfl::Generic::Array{int64_t{7}};
    EXPECT_THROW(static_cast<void>(decode(badType)), Exception);

    auto emptySignature = *context.reflect(graph);
    nativeOperator(calls(emptySignature)[0])["type"] = std::string{};
    EXPECT_THROW(static_cast<void>(decode(emptySignature)), Exception);

    auto duplicateExport = *context.reflect(graph);
    auto& exports = std::get<rfl::Generic::Array>(object(duplicateExport)["outputs"].variant());
    ASSERT_GE(exports.size(), 2U);
    exports[1] = exports[0];
    EXPECT_THROW(static_cast<void>(decode(duplicateExport)), Exception);

    const auto inputName = QualifiedIdentifier::parse("IN.VALUE");
    AsyncCallGraphBuilder inputBuilder{Schema<QualifiedUnboundField, Ordered>{QualifiedUnboundField{inputName, INTEGER}}};
    const auto inputCall = makeCall(inputBuilder, "Read", {inputBuilder.input(inputName)}, {INTEGER});
    inputBuilder.exportOutput(inputCall->output(0), QualifiedIdentifier::parse("OUT"));
    const auto inputWire = *context.reflect(inputBuilder.build());

    auto duplicateInput = inputWire;
    auto& schema = std::get<rfl::Generic::Array>(object(duplicateInput)["inputSchema"].variant());
    ASSERT_EQ(schema.size(), 1U);
    schema.push_back(schema.front());
    EXPECT_THROW(static_cast<void>(decode(duplicateInput)), Exception);

    auto unknownInput = inputWire;
    auto& inputArguments = std::get<rfl::Generic::Array>(object(calls(unknownInput).front())["arguments"].variant());
    object(inputArguments.front())["value"] = std::string{"MISSING"};
    EXPECT_THROW(static_cast<void>(decode(unknownInput)), Exception);

    auto emptyResults = inputWire;
    nativeConfig(calls(emptyResults).front())["resultTypes"] = rfl::Generic::Array{};
    EXPECT_THROW(static_cast<void>(decode(emptyResults)), Exception);

    auto undefinedType = inputWire;
    auto& undefinedTypes = std::get<rfl::Generic::Array>(nativeConfig(calls(undefinedType).front())["resultTypes"].variant());
    undefinedTypes.front() = rfl::Generic::Array{int64_t{0}, int64_t{0}};
    EXPECT_THROW(static_cast<void>(decode(undefinedType)), Exception);
}

TEST(AsyncCallGraphTest, MalformedWireFailuresUseDeserializationErrorCode)
{
    const ReflectionContext context;
    auto malformed = *context.reflect(diamond());
    nativeOperator(calls(malformed)[0])["type"] = std::string{};
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(decode(malformed)), ErrorCode::CannotDeserialize);
    auto dangling = *context.reflect(diamond());
    firstReference(calls(dangling)[1])["producer"] = std::string{"missing"};
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(decode(dangling)), ErrorCode::CannotDeserialize);
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
                    auto producer = makeCall(builder, "Constant", {value}, {dataType});
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

/// NOLINTEND(readability-magic-numbers)
