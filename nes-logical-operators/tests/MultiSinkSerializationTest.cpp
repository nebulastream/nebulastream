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

/// REPRODUCER for the multi-sink gap in QueryPlanSerializationUtil. These tests are EXPECTED TO FAIL on this
/// branch -- they document what is missing, they are not a regression.
///
/// Context: lowering was taught to handle multi-sink logical plans (multi-sink lowering PR), and issues #1753
/// (optimizer rules), #1755 (pipelining) and #1756 (executable plan / backpressure) track the remaining
/// end-to-end work. Serialization is a FOURTH single-root chokepoint that none of those three covers, and it
/// sits on the client submission path (GRPCQuerySubmissionBackend calls serializeQueryPlan), so it only bites
/// once a multi-sink query is submitted to a remote worker -- embedded single-node runs skip it entirely.
///
/// The single-root INVARIANT at the top of serializeQueryPlan has been removed on this branch. That alone does
/// NOT make multi-sink plans work; it converts a loud abort into SILENT DATA LOSS. What remains, per side:
///
/// serializeQueryPlan
///   - takes rootOperators.front() and runs BFSRange from that single root, so operators reachable only from a
///     second sink are never serialized;
///   - emits exactly one entry into the (already `repeated`) rootOperatorIds proto field.
///
/// deserializeQueryPlan
///   - builds ALL roots correctly from rootOperatorIds (that loop is already multi-root capable), then
///   - throws CannotDeserialize("Plan contains multiple root operators!") if more than one survived,
///   - validates only rootOperators.at(0) for the is-a-sink / has-children / has-descriptor conditions, and
///   - collapses the vector via `rootOperators = std::vector<LogicalOperator>{sink};`, discarding the rest.
///
/// Good news that the tests below also pin down: the DAG machinery already works. `alreadySerialized` dedupes
/// shared operators by OperatorId on the way out, and ReflectedPlan::getOperator memoizes into `builtOperators`
/// on the way back in, so a subplan shared by two sinks would round-trip as one shared instance rather than
/// being duplicated. Only the multi-root plumbing is missing.

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/UUID.hpp>
#include <DistributedQuery.hpp>
#include <QueryId.hpp>

namespace NES
{
namespace
{

/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class MultiSinkSerializationTest : public ::testing::Test
{
public:
    static Schema<UnqualifiedUnboundField, Ordered> createSchema()
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse("id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            {Identifier::parse("value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
    }

    static TypedLogicalOperator<SourceDescriptorLogicalOperator> makeSource(SourceCatalog& catalog, const std::string_view name)
    {
        const auto logical = catalog.addLogicalSource(Identifier::parse(std::string{name}), createSchema()).value();
        const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("file_path"), "/dev/null"}};
        const std::unordered_map<Identifier, std::string> parserConfig{{Identifier::parse("type"), "CSV"}};
        const auto descriptor
            = catalog.addPhysicalSource(logical, Identifier::parse("file"), Host{"localhost"}, sourceConfig, parserConfig).value();
        return SourceDescriptorLogicalOperator::create(descriptor);
    }

    static SinkDescriptor makeSinkDescriptor(SinkCatalog& sinkCatalog, const std::string_view name)
    {
        const std::unordered_map<Identifier, std::string> sinkConfig{
            {Identifier::parse("FILE_PATH"), "/dev/null"}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}};
        return sinkCatalog
            .addSinkDescriptor(
                Identifier::parse(std::string{name}), createSchema(), Identifier::parse("file"), Host{"localhost"}, sinkConfig, {})
            .value();
    }

    /// source -> {sinkA, sinkB}: one shared source instance feeding two sink roots.
    static LogicalPlan makeTwoSinkPlan(SourceCatalog& catalog, SinkCatalog& sinkCatalog)
    {
        const auto source = makeSource(catalog, "multisink_src");
        const auto sinkA = SinkLogicalOperator::create(makeSinkDescriptor(sinkCatalog, "sink_a")).withChildrenUnsafe({source});
        const auto sinkB = SinkLogicalOperator::create(makeSinkDescriptor(sinkCatalog, "sink_b")).withChildrenUnsafe({source});
        return LogicalPlan{
            QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()),
            {LogicalOperator{sinkA->withInferredSchema()}, LogicalOperator{sinkB->withInferredSchema()}}};
    }
};

/// Break #1 (serialize): only rootOperators.front() reaches the wire. The rootOperatorIds proto field is
/// already `repeated`, so the format itself is not the limitation -- the writer is.
TEST_F(MultiSinkSerializationTest, SerializeEmitsEveryRootId)
{
    SourceCatalog catalog;
    SinkCatalog sinkCatalog;
    const auto plan = makeTwoSinkPlan(catalog, sinkCatalog);
    ASSERT_EQ(plan.getRootOperators().size(), 2U) << "precondition: the plan under test really has two sink roots";

    const auto serialized = QueryPlanSerializationUtil::serializeQueryPlan(plan);

    EXPECT_EQ(serialized.rootoperatorids_size(), 2)
        << "serializeQueryPlan emitted only rootOperators.front(); the second sink root is absent from the wire format";
}

/// Break #2 (serialize): BFSRange starts at a single root, so an operator reachable only from the second sink
/// is never visited. Both sinks are serialized here only if the writer walks every root.
TEST_F(MultiSinkSerializationTest, SerializeVisitsOperatorsOfEveryRoot)
{
    SourceCatalog catalog;
    SinkCatalog sinkCatalog;
    const auto plan = makeTwoSinkPlan(catalog, sinkCatalog);

    const auto serialized = QueryPlanSerializationUtil::serializeQueryPlan(plan);

    /// source + sinkA + sinkB == 3 distinct operators; the shared source must appear exactly once.
    EXPECT_EQ(serialized.reflectedoperators_size(), 3)
        << "BFSRange walked only the first root, so operators reachable solely from the second sink were dropped";
}

/// Break #3 (round-trip): the end-to-end consequence a user would actually hit. With the INVARIANT gone this
/// does not throw -- it silently returns a plan with one fewer sink than was submitted.
TEST_F(MultiSinkSerializationTest, RoundTripPreservesBothSinks)
{
    SourceCatalog catalog;
    SinkCatalog sinkCatalog;
    const auto plan = makeTwoSinkPlan(catalog, sinkCatalog);

    const auto serialized = QueryPlanSerializationUtil::serializeQueryPlan(plan);
    const auto restored = QueryPlanSerializationUtil::deserializeQueryPlan(serialized);

    EXPECT_EQ(restored.getRootOperators().size(), 2U)
        << "the multi-sink plan lost a sink in the serialize/deserialize round trip, with no error reported";
}

/// Break #4 (deserialize): even when the wire format legitimately carries two roots, deserializeQueryPlan
/// rejects them outright ("Plan contains multiple root operators!") and then collapses the vector to
/// {rootOperators.at(0)}. Constructed by hand so it does not depend on the writer being fixed first.
TEST_F(MultiSinkSerializationTest, DeserializeAcceptsMultipleRootIds)
{
    SourceCatalog catalog;
    SinkCatalog sinkCatalog;
    const auto plan = makeTwoSinkPlan(catalog, sinkCatalog);

    /// Serialize each root separately, then merge into one payload carrying both roots and all operators.
    const auto onlySinkA = LogicalPlan{plan.getQueryId(), {plan.getRootOperators()[0]}};
    const auto onlySinkB = LogicalPlan{plan.getQueryId(), {plan.getRootOperators()[1]}};
    auto merged = QueryPlanSerializationUtil::serializeQueryPlan(onlySinkA);
    const auto partB = QueryPlanSerializationUtil::serializeQueryPlan(onlySinkB);
    for (const auto& reflectedOperator : partB.reflectedoperators())
    {
        merged.add_reflectedoperators(reflectedOperator);
    }
    for (const auto rootId : partB.rootoperatorids())
    {
        merged.add_rootoperatorids(rootId);
    }
    ASSERT_EQ(merged.rootoperatorids_size(), 2) << "precondition: the merged payload really carries two root ids";

    const auto restored = QueryPlanSerializationUtil::deserializeQueryPlan(merged);

    EXPECT_EQ(restored.getRootOperators().size(), 2U)
        << "deserializeQueryPlan built both roots, then rejected/discarded all but rootOperators.at(0)";
}

}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
