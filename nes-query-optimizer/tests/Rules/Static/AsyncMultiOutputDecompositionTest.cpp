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
#include <Rules/Static/AsyncOperatorDecompositionRule.hpp>

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <functional>
#include <ranges>
#include <variant>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Identifiers/Identifiers.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Util/DynamicBase.hpp>
#include <boost/asio/awaitable.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/AsyncLoweringContext.hpp>
#include <Operators/AsynchronousOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Reorderer.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Schema/Binder.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Util/Reflection/SharedPointerSerializationState.hpp>
#include <boost/asio/co_spawn.hpp> /// NOLINT(misc-include-cleaner) Public Asio provider; cleaner misattributes co_spawn.
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>
#include <AsyncGraphExecution.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncError.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/PreparedAsyncCall.hpp>
#include <Sources/AsyncSourceConfig.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Reflection.hpp>
#include <AsyncOperatorLoweringRegistry.hpp>
#include <AsyncOperatorUnreflectionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <OptimizerTestUtils.hpp>
#include <QueryOptimizerAsyncConfiguration.hpp>
#include <gtest/gtest.h>
#include <folly/hash/Hash.h>

namespace NES
{
const DataType integer{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
const DataType character{DataType::Type::CHAR, DataType::NULLABLE::NOT_NULLABLE};
const DataType nullableBytes{DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE};
std::atomic<size_t> nativeConstructions{0};
std::atomic<size_t> nativePreparations{0};
std::atomic<size_t> nativeAttempts{0};
std::atomic<size_t> nativeLowerings{0};

/// This deliberately ordinary logical node has no async base class. It owns one named LogicalFunction
/// and exercises the same inference/reorder/reflection contract as a plugin logical operator.
class TestMultiLogicalOperator final : public Reorderer, public ManagedByOperator, public AsynchronousOperator
{
public:
    TestMultiLogicalOperator(WeakLogicalOperator self, LogicalOperator child, LogicalFunction value)
        : ManagedByOperator(std::move(self)), child(std::move(child)), value(std::move(value))
    {
        infer();
    }

    static TypedLogicalOperator<TestMultiLogicalOperator> create(LogicalOperator child, LogicalFunction value);
    static AsyncOperator lowerToAsync(const LogicalOperator& op, AsyncLoweringContext& context);

    [[nodiscard]] std::string_view getName() const noexcept { return "TestMulti"; }

    [[nodiscard]] const detail::DynamicBase* getDynamicBase() const
    {
        return static_cast<const detail::DynamicBase*>(static_cast<const Castable<AsynchronousOperator>*>(this));
    }

    [[nodiscard]] const LogicalFunction& getValue() const { return value; }

    [[nodiscard]] LogicalOperator getChild() const { return child; }

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const { return {child}; }

    [[nodiscard]] TraitSet getTraitSet() const { return traits; }

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const
    {
        PRECONDITION(schema.has_value(), "TestMulti schema unavailable");
        return bindToOperator(self.lock(), *schema);
    }

    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider order) const override
    {
        auto fields = order(child) | RangeUnbinder{} | std::ranges::to<std::vector>();
        fields.emplace_back(Identifier::parse("OUT0"), value.getDataType());
        fields.emplace_back(Identifier::parse("OUT1"), character);
        fields.emplace_back(Identifier::parse("OUT2"), nullableBytes);
        return bindToOperator(self.lock(), Schema<UnqualifiedUnboundField, Ordered>{std::move(fields)});
    }

    [[nodiscard]] TestMultiLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const
    {
        PRECONDITION(children.size() == 1, "TestMulti requires one child");
        auto copy = *this;
        copy.child = std::move(children.front());
        return copy;
    }

    [[nodiscard]] TestMultiLogicalOperator withChildren(std::vector<LogicalOperator> children) const
    {
        auto copy = withChildrenUnsafe(std::move(children));
        copy.infer();
        return copy;
    }

    [[nodiscard]] TestMultiLogicalOperator withInferredSchema() const { return withChildren({child.withInferredSchema()}); }

    [[nodiscard]] TestMultiLogicalOperator withTraitSet(TraitSet valueTraits) const
    {
        auto copy = *this;
        copy.traits = std::move(valueTraits);
        return copy;
    }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const
    {
        return "TEST_MULTI(value: " + value.explain(verbosity) + ")";
    }

    [[nodiscard]] bool operator==(const TestMultiLogicalOperator& other) const
    {
        return StructuralFunctionEqual{}(value, other.value) && schema == other.schema && traits == other.traits;
    }

private:
    void infer()
    {
        TypeInferenceContext context{child.getOutputSchema()};
        value = context.inferType(value);
        auto fields = child.getOutputSchema() | RangeUnbinder{} | std::ranges::to<std::vector>();
        fields.emplace_back(Identifier::parse("OUT0"), value.getDataType());
        fields.emplace_back(Identifier::parse("OUT1"), character);
        fields.emplace_back(Identifier::parse("OUT2"), nullableBytes);
        auto result = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(std::move(fields));
        if (!result)
        {
            throw CannotInferSchema("TestMulti output collision");
        }
        schema = std::move(*result);
    }

    LogicalOperator child;
    LogicalFunction value;
    TraitSet traits;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> schema;
};

/// Config is all private and complete at construction; there is no default or late setter path.
class NativeTestMulti
{
public:
    NativeTestMulti(AsyncInputAccessor value, int marker, size_t privateLimit)
        : value(std::move(value)), marker(marker), privateLimit(privateLimit)
    {
        ++nativeConstructions;
    }

    [[nodiscard]] std::string_view getType() const { return "TEST_MULTI_NATIVE"; }

    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getWrittenFields() const
    {
        return {{Identifier::parse("OUT0"), value.getDataType()},
                {Identifier::parse("OUT1"), character},
                {Identifier::parse("OUT2"), nullableBytes}};
    }

    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext&, const AsyncRowHandle& row) const
    {
        ++nativePreparations;
        return std::unique_ptr<PreparedAsyncCall>{std::make_unique<Prepared>(value(row))};
    }

    [[nodiscard]] const AsyncInputAccessor& getValue() const { return value; }

    [[nodiscard]] int getMarker() const { return marker; }

    [[nodiscard]] size_t getPrivateLimit() const { return privateLimit; }

private:
    class Prepared final : public PreparedAsyncCall
    {
    public:
        explicit Prepared(RuntimeValue value) : value(std::move(value)) { }

        Prepared(const Prepared&) = delete;
        Prepared& operator=(const Prepared&) = delete;
        Prepared(Prepared&&) = delete;
        Prepared& operator=(Prepared&&) = delete;
        ~Prepared() override = default;

        boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> executeAttempt() override
        {
            ++nativeAttempts;
            co_return SharedRuntimeValues::make(value, RuntimeValue{'A'}, RuntimeValue::null(nullableBytes));
        }

    private:
        RuntimeValue value;
    };

    AsyncInputAccessor value;
    int marker;
    size_t privateLimit;
};

struct ReflectedNativeTestMulti
{
    AsyncInputAccessor value;
    int marker;
    size_t privateLimit;
};

struct ReflectedTestMultiLogical
{
    OperatorId operatorId;
    LogicalFunction value;
};

}

namespace std
{
template <>
struct hash<NES::TestMultiLogicalOperator>
{
    size_t operator()(const NES::TestMultiLogicalOperator& op) const noexcept
    {
        return folly::hash::hash_combine(0, NES::StructuralFunctionHash{}(op.getValue()));
    }
};
}

namespace NES
{
inline TypedLogicalOperator<TestMultiLogicalOperator> TestMultiLogicalOperator::create(LogicalOperator child, LogicalFunction value)
{
    return TypedLogicalOperator<TestMultiLogicalOperator>{std::move(child), std::move(value)};
}

template <>
struct Reflector<NativeTestMulti>
{
    Reflected operator()(const NativeTestMulti& op, const ReflectionContext& context) const
    {
        return context.reflect(ReflectedNativeTestMulti{op.getValue(), op.getMarker(), op.getPrivateLimit()});
    }
};

template <>
struct Unreflector<NativeTestMulti>
{
    NativeTestMulti operator()(const Reflected& reflected, const ReflectionContext& context) const
    {
        auto dto = context.unreflect<ReflectedNativeTestMulti>(reflected);
        return NativeTestMulti{std::move(dto.value), dto.marker, dto.privateLimit};
    }
};

template <>
struct Reflector<TypedLogicalOperator<TestMultiLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<TestMultiLogicalOperator>& op, const ReflectionContext& context) const
    {
        return context.reflect(ReflectedTestMultiLogical{op.getId(), op->getValue()});
    }
};

template <>
struct Unreflector<TypedLogicalOperator<TestMultiLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;

    explicit Unreflector(ContextType value) : plan(std::move(value)) { }

    TypedLogicalOperator<TestMultiLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const
    {
        auto dto = context.unreflect<ReflectedTestMultiLogical>(reflected);
        auto children = plan->getChildrenFor(dto.operatorId, context);
        if (children.size() != 1)
        {
            throw CannotDeserialize("TestMulti requires one child");
        }
        return TestMultiLogicalOperator::create(std::move(children.front()), std::move(dto.value));
    }
};

AsyncOperator TestMultiLogicalOperator::lowerToAsync(const LogicalOperator& op, AsyncLoweringContext& context)
{
    ++nativeLowerings;
    return NativeTestMulti{context.lower(op.getAs<TestMultiLogicalOperator>()->getValue()), 713, 17};
}

namespace
{
LogicalOperator withPlacement(const LogicalOperator& op)
{
    auto traits = op.getTraitSet();
    traits.insert(PlacementTrait{Host{"localhost"}});
    traits.insert(MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});
    const Schema<UnqualifiedUnboundField, Ordered> order{op.getOutputSchema()
                                                         | std::views::transform([](const Field& f) { return f.unbound(); })};
    traits.insert(FieldOrderingTrait{order});
    if (!traits.contains<FieldMappingTrait>())
    {
        std::unordered_map<UnqualifiedUnboundField, QualifiedIdentifier> mapping;
        for (const auto& field : order)
        {
            mapping.emplace(field, QualifiedIdentifier{field.getFullyQualifiedName()});
        }
        traits.insert(FieldMappingTrait{std::move(mapping)});
    }
    return op.withTraitSet(std::move(traits));
}

std::vector<TypedLogicalOperator<SourceDescriptorLogicalOperator>> asyncSources(const LogicalPlan& plan)
{
    auto result = getOperatorByType<SourceDescriptorLogicalOperator>(plan);
    std::erase_if(result, [](const auto& source) { return source->getSourceDescriptor().getSourceType() != Identifier::parse("Async"); });
    return result;
}

void registerNative()
{
    static_cast<void>(AsyncOperatorUnreflectionRegistry::instance().addEntry("TEST_MULTI_NATIVE",
                                                                             [](const Reflected& value, const ReflectionContext& context)
                                                                             { return context.unreflect<NativeTestMulti>(value); }));
    static_cast<void>(AsyncOperatorLoweringRegistry::instance().addEntry("TestMulti",
                                                                         [](const LogicalOperator& op, AsyncLoweringContext& context)
                                                                         { return TestMultiLogicalOperator::lowerToAsync(op, context); }));
}
}

TEST(AsyncMultiOutputDecompositionTest, LowersNamedMultiResultTypesAndPreservesLimitsAndMetadata)
{
    registerNative();
    OptimizerTestUtils utils;
    const auto source = withPlacement(utils.createSource("multi_input", std::vector<std::string>{"seed"}));
    const auto seed = source.getOutputSchema().getFieldByName(Identifier::parse("seed")).value();
    const auto logical = withPlacement(TestMultiLogicalOperator::create(source, FieldAccessLogicalFunction{seed}));
    const auto plan
        = utils.createPlan(utils.createSink(logical, "multi_output", logical.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields()));
    const AsyncOperatorDecompositionRule rule{
        QueryOptimizerAsyncConfiguration{4, 9}, std::make_shared<SourceCatalog>(), std::make_shared<SinkCatalog>()};
    const auto lowered = rule.apply(plan);
    const auto sources = asyncSources(lowered);
    ASSERT_EQ(sources.size(), 1);
    const auto& config = sources.front()->getSourceDescriptor().getPluginData().getAs<AsyncSourceConfig>();
    EXPECT_EQ(config.maxConcurrency, 4);
    EXPECT_EQ(config.channel->getCapacity(), 9);
    ASSERT_EQ(config.calls.getCalls().size(), 1);
    const auto native = config.calls.getCalls().front()->getAsyncOperator().getAs<NativeTestMulti>();
    EXPECT_EQ(native->getMarker(), 713);
    EXPECT_EQ(native->getPrivateLimit(), 17);
    EXPECT_EQ(config.calls.getCalls().front()->getResultTypes(), (std::vector<DataType>{seed.getDataType(), character, nullableBytes}));
    EXPECT_EQ(std::get<InputFieldReference>(native->getValue().getSource()).getDataType(), seed.getDataType());
    const auto handoffSink = lowered.getRootOperators().front().getAs<SinkLogicalOperator>();
    const auto padded = handoffSink->getChild().getAs<ProjectionLogicalOperator>();
    const auto projections = padded->getProjections();
    const auto charPadding = std::ranges::find_if(
        projections, [](const auto& projection) { return projection.first.getFullyQualifiedName() == QualifiedIdentifier::parse("OUT1"); });
    ASSERT_NE(charPadding, projections.end());
    ASSERT_TRUE(charPadding->second.template tryGetAs<ConstantValueLogicalFunction>().has_value());
    EXPECT_EQ(charPadding->second.getAs<ConstantValueLogicalFunction>()->getConstantValue(), std::string(1, '\0'));
}

TEST(AsyncMultiOutputDecompositionTest, DoesNotConstructNativeDuringLogicalInferenceOrReflection)
{
    registerNative();
    OptimizerTestUtils utils;
    const auto source = utils.createSource("inference_input", std::vector<std::string>{"seed"});
    const auto constructions = nativeConstructions.load();
    auto op
        = TestMultiLogicalOperator::create(source, FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("seed")].value()});
    EXPECT_EQ(op->getOutputSchema().size(), 4);
    const auto inferred = op.withInferredSchema();
    const ReflectionContext writer{std::make_shared<SharedPointerSerializationState>()};
    const auto reflected = writer.reflect(inferred);
    static_cast<void>(reflected);
    EXPECT_EQ(nativeConstructions.load(), constructions);
    EXPECT_EQ(op->getValue().getDataType(), source.getOutputSchema()[Identifier::parse("seed")]->getDataType());
}

TEST(AsyncMultiOutputDecompositionTest, RoundTripsPrivateNativeConfigurationAndFieldAccessorWithoutFactoryReplay)
{
    registerNative();
    AsyncCallGraphBuilder builder{{QualifiedUnboundField{Identifier::parse("seed"), integer}}};
    auto call = builder.call(NativeTestMulti{AsyncInputAccessor{builder.input(QualifiedIdentifier::parse("seed"))}, 991, 23}, {});
    builder.require(call);
    builder.exportOutput(call->output(0), QualifiedIdentifier::parse("OUT0"));
    auto graph = builder.build();
    const auto lowerings = nativeLowerings.load();
    const auto copy = ReflectionContext{}.unreflect<AsyncCallGraph>(ReflectionContext{}.reflect(graph));
    EXPECT_EQ(nativeLowerings.load(), lowerings);
    ASSERT_EQ(copy.getCalls().size(), 1);
    const auto native = copy.getCalls().front()->getAsyncOperator().getAs<NativeTestMulti>();
    EXPECT_EQ(native->getMarker(), 991);
    EXPECT_EQ(native->getPrivateLimit(), 23);
    EXPECT_TRUE(std::holds_alternative<InputFieldReference>(native->getValue().getSource()));

    /// Required effect roots survive even when none of their output values are exported.
    AsyncCallGraphBuilder effects{{}};
    const auto effect = effects.call(NativeTestMulti{AsyncInputAccessor{RuntimeValue{int64_t{5}}}, 17, 3}, {});
    effects.require(effect);
    const auto effectGraph = effects.build();
    EXPECT_TRUE(effectGraph.getOutputs().empty());
    ASSERT_EQ(effectGraph.getRequiredCalls().size(), 1);
    EXPECT_EQ(effectGraph.getRequiredCalls().front(), effect);
}

TEST(AsyncMultiOutputDecompositionTest, RequiredMultiOutputInvocationExecutesOncePerRowWhenAllResultsAreDiscarded)
{
    registerNative();
    nativePreparations = 0;
    nativeAttempts = 0;
    AsyncCallGraphBuilder builder{{}};
    const auto call = builder.call(NativeTestMulti{AsyncInputAccessor{RuntimeValue{int64_t{5}}}, 17, 3}, {});
    builder.require(call);
    const auto graph = std::make_shared<const AsyncCallGraph>(builder.build());
    boost::asio::io_context executor;
    const auto strand = boost::asio::make_strand(executor);
    for (size_t row = 0; row < 2; ++row)
    {
        auto execution = std::make_shared<AsyncGraphExecution>(
            strand,
            graph,
            [](const InputFieldReference&) -> RuntimeValue
            {
                ADD_FAILURE() << "Unexpected tuple input";
                return RuntimeValue{int64_t{0}};
            },
            [](AsyncCall::Ptr node, AsyncRowHandle values) { return invokeOnce(node->getAsyncOperator(), std::move(values)); });
        /// NOLINTNEXTLINE(misc-include-cleaner) co_spawn is declared by the directly included public boost/asio/co_spawn.hpp.
        auto result = boost::asio::co_spawn(strand, execution->run(), boost::asio::use_future);
        executor.run();
        const auto outcome = result.get();
        ASSERT_TRUE(outcome);
        EXPECT_TRUE(outcome->empty());
        executor.restart();
    }
    EXPECT_EQ(nativePreparations.load(), 2);
    EXPECT_EQ(nativeAttempts.load(), 2);
}

TEST(AsyncMultiOutputDecompositionTest, DecompositionRejectsCustomOutputCollisionAndKeepsEveryInvocationDistinct)
{
    registerNative();
    OptimizerTestUtils utils;
    const auto source = withPlacement(utils.createSource("collision_input", std::vector<std::string>{"OUT1"}));
    EXPECT_THROW(static_cast<void>(TestMultiLogicalOperator::create(
                     source, FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("OUT1")].value()})),
                 Exception);

    const auto ordinarySource = withPlacement(utils.createSource("distinct_input", std::vector<std::string>{"seed"}));
    auto first = withPlacement(TestMultiLogicalOperator::create(
        ordinarySource, FieldAccessLogicalFunction{ordinarySource.getOutputSchema()[Identifier::parse("seed")].value()}));
    const auto forwarded = withPlacement(ProjectionLogicalOperator::create(
        first,
        {{Identifier::parse("seed"), FieldAccessLogicalFunction{first.getOutputSchema()[Identifier::parse("seed")].value()}}},
        ProjectionLogicalOperator::Asterisk{false}));
    const auto second = withPlacement(TestMultiLogicalOperator::create(
        forwarded, FieldAccessLogicalFunction{forwarded.getOutputSchema()[Identifier::parse("seed")].value()}));
    const auto plan
        = utils.createPlan(utils.createSink(second, "distinct_output", second.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields()));
    const AsyncOperatorDecompositionRule rule{QueryOptimizerAsyncConfiguration{std::nullopt, std::nullopt},
                                              std::make_shared<SourceCatalog>(),
                                              std::make_shared<SinkCatalog>()};
    const auto lowered = rule.apply(plan);
    const auto sources = asyncSources(lowered);
    ASSERT_EQ(sources.size(), 1);
    const auto& graph = sources.front()->getSourceDescriptor().getPluginData().getAs<AsyncSourceConfig>().calls;
    EXPECT_EQ(graph.getCalls().size(), 2);
    EXPECT_EQ(graph.getRequiredCalls().size(), 2);
    EXPECT_NE(graph.getCalls()[0], graph.getCalls()[1]);
    EXPECT_TRUE(graph.getCalls()[0]->getDependencies().empty());
    EXPECT_TRUE(graph.getCalls()[1]->getDependencies().empty());
}
}
