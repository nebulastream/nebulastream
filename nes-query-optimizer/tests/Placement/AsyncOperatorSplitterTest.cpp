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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <Async/AsyncWiring.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Placement/AsyncOperatorSplitter.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Traits/AsyncExecutionTrait.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <QueryId.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

/// Exercises the plan rewrite in isolation: a plan containing an operator marked for
/// asynchronous execution must come out as two plans on the same worker, joined by a
/// handoff channel, with the wiring the consumer source needs in its descriptor.
class AsyncOperatorSplitterTest : public Testing::BaseUnitTest
{
public:
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<SinkCatalog> sinkCatalog;
    Host host{"localhost:8080"};

    /// BaseUnitTest flushes the logger on teardown, so it has to exist.
    static void SetUpTestSuite() { Logger::setupLogging("AsyncOperatorSplitterTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        sourceCatalog = std::make_shared<SourceCatalog>();
        sinkCatalog = std::make_shared<SinkCatalog>();
    }

    /// reviewId, reviewText — what arrives from the stream.
    static Schema<UnqualifiedUnboundField, Ordered> inputSchema()
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("reviewId"), DataType::Type::UINT64},
            UnqualifiedUnboundField{Identifier::parse("reviewText"), DataType::Type::VARSIZED}};
    }

    /// What the marked operator declares as its output. Deliberately the same fields in a
    /// different order: the stand-in operator below is a pass-through, so the field *set* has to
    /// match for schema inference, while the differing order still proves that the splitter reads
    /// the output schema from the operator and the channel schema from its child.
    static Schema<UnqualifiedUnboundField, Ordered> outputSchema()
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("reviewText"), DataType::Type::VARSIZED},
            UnqualifiedUnboundField{Identifier::parse("reviewId"), DataType::Type::UINT64}};
    }

    static AsyncExecutionTrait sentimentTrait()
    {
        return AsyncExecutionTrait{
            "Delay",
            {{"input_field", "reviewText"}, {"output_field", "sentiment"}, {"delay_ms", "100"}},
            /*batchSize*/ 10,
            /*maxConcurrency*/ 4,
            /*channelCapacity*/ 32,
            /*preserveOrder*/ true};
    }

    LogicalOperator makeSource(const std::string& name, const Schema<UnqualifiedUnboundField, Ordered>& schema)
    {
        const auto logical = sourceCatalog->addLogicalSource(Identifier::parse(name), schema).value();
        const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("file_path"), "/dev/null"}};
        const std::unordered_map<Identifier, std::string> parserConfig{{Identifier::parse("type"), "CSV"}};
        const auto descriptor
            = sourceCatalog->addPhysicalSource(logical, Identifier::parse("file"), host, sourceConfig, parserConfig).value();

        TraitSet traits;
        traits.insert(FieldOrderingTrait{schema});
        traits.insert(OutputOriginIdsTrait{{OriginId{1}}});
        return SourceDescriptorLogicalOperator::create(descriptor)->withTraitSet(traits);
    }

    LogicalOperator makeSink(const LogicalOperator& child, const Schema<UnqualifiedUnboundField, Ordered>& schema)
    {
        const std::unordered_map<Identifier, std::string> config{
            {Identifier::parse("file_path"), "/dev/null"}, {Identifier::parse("output_format"), "CSV"}};
        const auto descriptor
            = sinkCatalog->addSinkDescriptor(Identifier::parse("result"), schema, Identifier::parse("File"), host, config, {}).value();

        TraitSet traits;
        traits.insert(FieldOrderingTrait{schema});
        /// Inference from the root down, the way a real plan reaches the placement phases. Without
        /// it the operators carry no output schema and the first rewrite trips over that.
        return SinkLogicalOperator::create(child, descriptor)->withTraitSet(traits).withInferredSchema();
    }

    /// Source -> marked operator -> sink, as one local plan on one host.
    DistributedLogicalPlan makePlanWithAsyncOperator(const AsyncExecutionTrait& trait, const OriginId operatorOrigin = OriginId{7})
    {
        const auto source = makeSource("reviews", inputSchema());

        TraitSet asyncTraits;
        asyncTraits.insert(FieldOrderingTrait{outputSchema()});
        asyncTraits.insert(OutputOriginIdsTrait{{operatorOrigin}});
        asyncTraits.insert(trait);
        const LogicalOperator asyncOperator
            = IngestionTimeWatermarkAssignerLogicalOperator::create(source)->withTraitSet(asyncTraits);

        const auto sink = makeSink(asyncOperator, inputSchema());
        const LogicalPlan plan{INVALID_QUERY_ID, {sink}};
        return DistributedLogicalPlan{{{host, {plan}}}, plan};
    }

    [[nodiscard]] AsyncOperatorSplitter splitter() const
    {
        return AsyncOperatorSplitter{
            SharedPtr<const SourceCatalog>{sourceCatalog}, SharedPtr<const SinkCatalog>{sinkCatalog}};
    }

    /// The single leaf of a plan.
    static LogicalOperator leafOf(const LogicalOperator& root)
    {
        LogicalOperator current = root;
        while (!current.getChildren().empty())
        {
            current = current.getChildren().front();
        }
        return current;
    }
};

TEST_F(AsyncOperatorSplitterTest, LeavesPlansWithoutMarkedOperatorsAlone)
{
    const auto source = makeSource("reviews", inputSchema());
    const auto sink = makeSink(source, inputSchema());
    const LogicalPlan plan{INVALID_QUERY_ID, {sink}};

    const auto result = splitter().split(DistributedLogicalPlan{{{host, {plan}}}, plan});

    EXPECT_EQ(result.size(), 1U);
    EXPECT_EQ(result[host].size(), 1U);
}

TEST_F(AsyncOperatorSplitterTest, SplitsIntoProducerAndConsumerOnTheSameHost)
{
    const auto result = splitter().split(makePlanWithAsyncOperator(sentimentTrait()));

    /// One plan in, two out — and both on the worker the operator was placed on, because the
    /// channel only exists inside a single process.
    ASSERT_EQ(result.size(), 2U);
    const auto& plans = result[host];
    ASSERT_EQ(plans.size(), 2U);

    /// The producer half ends in the handoff sink.
    const auto producerRoot = plans.at(0).getRootOperators().front();
    const auto producerSink = producerRoot.tryGetAs<SinkLogicalOperator>();
    ASSERT_TRUE(producerSink.has_value());
    /// Type names are canonical identifiers, hence upper case.
    EXPECT_EQ(producerSink.value()->getSinkDescriptor()->getSinkType(), "HANDOFF");

    /// The consumer half begins with the async source and still ends in the real sink.
    const auto consumerRoot = plans.at(1).getRootOperators().front();
    const auto consumerSink = consumerRoot.tryGetAs<SinkLogicalOperator>();
    ASSERT_TRUE(consumerSink.has_value());
    EXPECT_EQ(consumerSink.value()->getSinkDescriptor()->getSinkType(), "FILE");

    const auto consumerLeaf = leafOf(consumerRoot).tryGetAs<SourceDescriptorLogicalOperator>();
    ASSERT_TRUE(consumerLeaf.has_value());
    EXPECT_EQ(consumerLeaf.value()->getSourceDescriptor().getSourceType(), "ASYNC");

    /// The marked operator itself is gone from both halves; the source runs it now.
    for (const auto& plan : plans)
    {
        for (const auto& root : plan.getRootOperators())
        {
            EXPECT_FALSE(hasTrait<AsyncExecutionTrait>(leafOf(root).getTraitSet()))
                << "the operator must not survive the split as an operator";
        }
    }
}

TEST_F(AsyncOperatorSplitterTest, BothHalvesShareOneChannelId)
{
    const auto result = splitter().split(makePlanWithAsyncOperator(sentimentTrait()));
    const auto& plans = result[host];
    ASSERT_EQ(plans.size(), 2U);

    const auto sinkDescriptor = plans.at(0).getRootOperators().front().getAs<SinkLogicalOperator>()->getSinkDescriptor().value();
    const auto sourceDescriptor
        = leafOf(plans.at(1).getRootOperators().front()).getAs<SourceDescriptorLogicalOperator>()->getSourceDescriptor();

    const auto sinkChannel = sinkDescriptor.tryGetFromConfig<std::string>("CHANNEL");
    const auto sourceChannel = sourceDescriptor.tryGetFromConfig<std::string>("CHANNEL");
    ASSERT_TRUE(sinkChannel.has_value());
    ASSERT_TRUE(sourceChannel.has_value());
    EXPECT_FALSE(sinkChannel->empty());
    /// The only thing the two halves have to agree on to find each other.
    EXPECT_EQ(sinkChannel.value(), sourceChannel.value());
}

TEST_F(AsyncOperatorSplitterTest, SourceDescriptorCarriesTheOperatorConfiguration)
{
    const auto trait = sentimentTrait();
    const auto result = splitter().split(makePlanWithAsyncOperator(trait));
    const auto sourceDescriptor
        = leafOf(result[host].at(1).getRootOperators().front()).getAs<SourceDescriptorLogicalOperator>()->getSourceDescriptor();

    EXPECT_EQ(sourceDescriptor.tryGetFromConfig<std::string>("EXECUTOR_TYPE").value(), trait.executorType);
    EXPECT_EQ(sourceDescriptor.tryGetFromConfig<size_t>("BATCH_SIZE").value(), trait.batchSize);
    EXPECT_EQ(sourceDescriptor.tryGetFromConfig<size_t>("MAX_CONCURRENCY").value(), trait.maxConcurrency);
    EXPECT_EQ(sourceDescriptor.tryGetFromConfig<bool>("PRESERVE_ORDER").value(), trait.preserveOrder);

    /// The operator's own settings survive the trip through the descriptor.
    const auto config = decodeConfig(sourceDescriptor.tryGetFromConfig<std::string>("EXECUTOR_CONFIG").value());
    EXPECT_EQ(config, trait.config);

    /// The descriptor's schema is what the source produces; the schema arriving through the
    /// channel has to travel separately, or the source could not read the incoming records.
    const auto encodedInput = sourceDescriptor.tryGetFromConfig<std::string>("INPUT_SCHEMA").value();
    EXPECT_EQ(decodeSchema(encodedInput), inputSchema());
    EXPECT_EQ(*sourceDescriptor.getLogicalSource().getSchema(), outputSchema());
}

TEST_F(AsyncOperatorSplitterTest, ConsumerSourceKeepsTheOperatorsOriginId)
{
    /// Renumbering here would either crash a downstream watermark processor with an unknown
    /// origin or stall it forever, so the source inherits the operator's origin ids.
    constexpr OriginId operatorOrigin{7};
    const auto result = splitter().split(makePlanWithAsyncOperator(sentimentTrait(), operatorOrigin));
    const auto consumerLeaf = leafOf(result[host].at(1).getRootOperators().front());

    const auto originIds = consumerLeaf.getTraitSet().tryGet<OutputOriginIdsTrait>();
    ASSERT_TRUE(originIds.has_value());
    ASSERT_EQ(originIds.value()->size(), 1U);
    EXPECT_EQ((*originIds.value())[0], operatorOrigin);
}

TEST_F(AsyncOperatorSplitterTest, TwoMarkedOperatorsProduceThreePlans)
{
    /// source -> async -> async -> sink
    const auto source = makeSource("reviews", inputSchema());

    TraitSet firstTraits;
    firstTraits.insert(FieldOrderingTrait{outputSchema()});
    firstTraits.insert(OutputOriginIdsTrait{{OriginId{7}}});
    firstTraits.insert(sentimentTrait());
    const LogicalOperator first = IngestionTimeWatermarkAssignerLogicalOperator::create(source)->withTraitSet(firstTraits);

    TraitSet secondTraits;
    secondTraits.insert(FieldOrderingTrait{outputSchema()});
    secondTraits.insert(OutputOriginIdsTrait{{OriginId{8}}});
    secondTraits.insert(sentimentTrait());
    const LogicalOperator second = IngestionTimeWatermarkAssignerLogicalOperator::create(first)->withTraitSet(secondTraits);

    const auto sink = makeSink(second, inputSchema());
    const LogicalPlan plan{INVALID_QUERY_ID, {sink}};

    const auto result = splitter().split(DistributedLogicalPlan{{{host, {plan}}}, plan});

    /// Each marked operator adds one plan, and the rule applies to the producer halves too.
    EXPECT_EQ(result[host].size(), 3U);
}

}
