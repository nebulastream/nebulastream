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

/// The following tests test the construction of a diamond shaped topology: one source feeding two branches that a union merges again.
/// This shape is currently neither obtainalbe from a SQL query nor can it successfully be executed because the union operator would
/// receive duplicate sequence numbers with the same origin id (see Issue #1958).
/// Builds plans by hand and runs them on a worker,  shapes SQL cannot currently express.
///
///        SELECTION(value > 3)
///       /                     \
///   SOURCE                     UNION -> SINK
///       \                     /
///        SELECTION(value < 2)
///
/// PipeliningPhaseFanOutTest pins down what the pipelining phase makes of that, structurally, on wrappers
/// holding pass-through operators. These tests are the counterpart with real operators, real lowering and real
/// execution, checking the rows that come out the other end.
///
/// A GTest rather than a systest because SQL does not produce the shape: sub-plan sharing arises from a query
/// writing into several sinks, whose branches end in different sinks instead of converging, and a UNION over one
/// logical source binds to two separate source operators rather than sharing one. SingleNodeWorker::startQuery
/// takes a LogicalPlan and does not re-optimize it, so what is built here is what runs.
///
/// The diamond compiles, deploys and runs, but only over an empty source: with rows flowing it is rejected at the
/// merge point, by an INVARIANT that cannot be caught. See the comment on that test. The union-over-separate-sources
/// test is the control that keeps the harness honest: the same query shape without the sharing, carrying real rows
/// end to end.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ModelCatalog.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace
{

class SharedOperatorPlanExecutionTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SharedOperatorPlanExecutionTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        workDir = std::filesystem::temp_directory_path() / ("shared-operator-plan-" + std::to_string(::getpid()));
        std::filesystem::create_directories(workDir);
        inputPath = workDir / "input.csv";
        emptyInputPath = workDir / "empty.csv";
        outputPath = workDir / "output.csv";
        std::ofstream{emptyInputPath}.close();
        std::ofstream input(inputPath);
        input << "1,1,12\n1,2,23\n1,3,34\n1,4,45\n1,5,56\n";
        input.close();
    }

    void TearDown() override
    {
        std::filesystem::remove_all(workDir);
        BaseUnitTest::TearDown();
    }

    static Schema<UnqualifiedUnboundField, Ordered> createSchema()
    {
        std::vector<UnqualifiedUnboundField> fields;
        fields.emplace_back(Identifier::parse("id"), DataTypeProvider::provideDataType(DataType::Type::UINT64));
        fields.emplace_back(Identifier::parse("value"), DataTypeProvider::provideDataType(DataType::Type::UINT64));
        fields.emplace_back(Identifier::parse("timestamp"), DataTypeProvider::provideDataType(DataType::Type::UINT64));
        return Schema<UnqualifiedUnboundField, Ordered>{fields};
    }

    TypedLogicalOperator<SourceDescriptorLogicalOperator> createSource(const std::string& name, const std::filesystem::path& path = {})
    {
        const auto logicalSource = sourceCatalog->addLogicalSource(Identifier::parse(name), createSchema());
        EXPECT_TRUE(logicalSource.has_value());
        const std::unordered_map<Identifier, std::string> sourceConfig{
            {Identifier::parse("FILE_PATH"), path.empty() ? inputPath.string() : path.string()}};
        const std::unordered_map<Identifier, std::string> parserConfig{{Identifier::parse("TYPE"), "CSV"}};
        auto descriptor = sourceCatalog->addPhysicalSource(
            logicalSource.value(), Identifier::parse("File"), Host{"localhost"}, sourceConfig, parserConfig);
        EXPECT_TRUE(descriptor.has_value());
        return SourceDescriptorLogicalOperator::create(std::move(descriptor.value()));
    }

    SinkDescriptor createFileSink(const std::string& name)
    {
        const std::unordered_map<Identifier, std::string> sinkConfig{
            {Identifier::parse("FILE_PATH"), outputPath.string()}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}};
        auto descriptor = sinkCatalog->addSinkDescriptor(
            Identifier::parse(name), createSchema(), Identifier::parse("File"), Host{"localhost"}, sinkConfig, {});
        EXPECT_TRUE(descriptor.has_value());
        return descriptor.value();
    }

    static LogicalFunction fieldCompare(const LogicalOperator& child, const std::string& field, const std::string& literal, bool greater)
    {
        const auto access = FieldAccessLogicalFunction{child.getOutputSchema()[Identifier::parse(field)].value()};
        const auto constant = ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, literal};
        if (greater)
        {
            return GreaterLogicalFunction{access, constant};
        }
        return LessLogicalFunction{access, constant};
    }

    static LogicalOperator leafOf(LogicalOperator op)
    {
        while (!op.getChildren().empty())
        {
            op = op.getChildren()[0];
        }
        return op;
    }

    /// Counts how often opId occurs among the descendants of root, following every edge. A shared operator is
    /// reached once per path, so a diamond apex shows up twice while a plain tree node shows up once.
    static size_t countReachable(const LogicalOperator& root, const OperatorId opId)
    {
        size_t count = root.getId() == opId ? 1 : 0;
        for (const auto& child : root.getChildren())
        {
            count += countReachable(child, opId);
        }
        return count;
    }

    /// Submits the plan and waits for it to settle. Returns nullopt if the worker never accepted it, i.e. if it did
    /// not survive compilation, and otherwise the state it came to rest in.
    static std::optional<QueryStatus> runToTermination(LogicalPlan plan)
    {
        SingleNodeWorkerConfiguration configuration{};
        SingleNodeWorker worker{configuration};

        const auto queryId = worker.startQuery(std::move(plan));
        if (!queryId.has_value())
        {
            return std::nullopt;
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        QueryStatus state = QueryStatus::Registered;
        while (std::chrono::steady_clock::now() < deadline)
        {
            const auto status = worker.getQueryStatus(queryId.value());
            if (status.has_value())
            {
                state = status->state;
                if (state == QueryStatus::Stopped || state == QueryStatus::Failed)
                {
                    break;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return state;
    }

    std::vector<std::string> runToCompletion(LogicalPlan plan) const
    {
        const auto state = runToTermination(std::move(plan));
        EXPECT_TRUE(state.has_value()) << "the worker refused the plan";
        EXPECT_EQ(state.value_or(QueryStatus::Failed), QueryStatus::Stopped) << "query did not run to completion";

        /// The CSV sink writes a schema header first ("ID:UINT64:NOT_NULLABLE,..."); only the rows are of interest.
        std::vector<std::string> lines;
        std::ifstream output(outputPath);
        std::string line;
        while (std::getline(output, line))
        {
            if (!line.empty() and line.find(':') == std::string::npos)
            {
                lines.push_back(line);
            }
        }
        return lines;
    }

    std::filesystem::path workDir;
    std::filesystem::path inputPath;
    std::filesystem::path emptyInputPath;
    std::filesystem::path outputPath;
    std::shared_ptr<SourceCatalog> sourceCatalog = std::make_shared<SourceCatalog>();
    std::shared_ptr<SinkCatalog> sinkCatalog = std::make_shared<SinkCatalog>();
    std::shared_ptr<ModelCatalog> modelCatalog = std::make_shared<ModelCatalog>();
};

/// Control: the same union, but each branch reading a source of its own — the shape a SQL UNION over one logical
/// source actually binds to. Nothing is shared, so this is a tree, and it must pass for the diamond result below
/// to say anything about sharing rather than about unions in general.
TEST_F(SharedOperatorPlanExecutionTest, UnionOverSeparateSourcesExecutes)
{
    const auto highSource = createSource("stream_high");
    const auto lowSource = createSource("stream_low");
    const auto high = SelectionLogicalOperator::create(highSource, fieldCompare(highSource, "value", "3", true));
    const auto low = SelectionLogicalOperator::create(lowSource, fieldCompare(lowSource, "value", "2", false));
    const auto merged = UnionLogicalOperator::create({LogicalOperator{high}, LogicalOperator{low}});
    const auto sink = SinkLogicalOperator::create(merged, createFileSink("union_sink"));

    auto plan = LogicalPlan{QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}), {sink}};
    auto optimized = RuleBasedOptimizer{QueryOptimizerConfiguration{}, sourceCatalog, sinkCatalog, modelCatalog}.optimize(std::move(plan));

    const auto lines = runToCompletion(std::move(optimized));

    std::vector<std::string> sorted = lines;
    std::ranges::sort(sorted);
    const std::vector<std::string> expected{"1,1,12", "1,4,45", "1,5,56"};
    EXPECT_EQ(sorted, expected);
}

/// A diamond compiles, deploys and runs — over an empty source, deliberately.
///
/// With rows flowing, the shape dies at the merge point:
///
///   EmitOperatorHandler.cpp: Invariant violated: (!completedSequencesLock->contains(seqNumberOriginId)):
///   Received chunk for sequence { seqNumber = 1, originId = 2} that was already completed
///
/// Both branches relay the origin of the operator they share, so they carry the same origin and sequence numbers
/// into the union, and sequence numbers are only unique within an origin. Network sinks already avoid this by
/// giving each channel origins of its own (ConfigParametersNetworkSink::ORIGIN_ID_MAP); a fan-out that stays on
/// one node has no equivalent, so the merge point sees the same sequence twice.
///
/// That failure is INVARIANT, i.e. std::terminate() rather than an exception: it cannot be caught, the query never
/// reaches QueryStatus::Failed, and the process aborts. Asserting it needs a death test, and the invariant spends
/// ~17s symbolizing a stack trace before dying, so that costs more than the rest of this suite put together. What
/// is asserted here instead is everything up to the data path: the plan is a genuine diamond, the worker accepts
/// it, and it compiles, deploys and starts. The empty source is what keeps that deterministic — no rows means no
/// second chunk at the union, so there is no race between reaching Stopped and the invariant firing.
///
/// The pipelining phase itself builds the right structure for this shape — PipeliningPhaseFanOutTest covers that,
/// and the failure is downstream of it — so this is not a regression of the fan-out work. Once the same-node case
/// gets its own origins, point this at inputPath instead and assert the sink received the rows of both branches,
/// "1,1,12", "1,4,45" and "1,5,56".
///
/// The diamond is grafted onto an already optimized single-branch plan rather than optimized as a whole: handed
/// a shared source the rule based optimizer rewrites it into one source operator per branch, which is a tree
/// again. Grafting afterwards is the same trick MultiRootLoweringTest uses to get a DAG past the optimizer.
/// Note also that withInferredSchema() regenerates operator ids and would silently undo the sharing.
TEST_F(SharedOperatorPlanExecutionTest, DiamondOverSharedSourceCompilesDeploysAndRuns)
{
    const auto source = createSource("stream", emptyInputPath);
    const auto high = SelectionLogicalOperator::create(source, fieldCompare(source, "value", "3", true));
    const auto singleBranchSink = SinkLogicalOperator::create(high, createFileSink("diamond_sink"));
    auto singleBranch = LogicalPlan{
        QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}), {singleBranchSink}};
    const auto optimizedSingleBranch
        = RuleBasedOptimizer{QueryOptimizerConfiguration{}, sourceCatalog, sinkCatalog, modelCatalog}.optimize(std::move(singleBranch));

    ASSERT_EQ(optimizedSingleBranch.getRootOperators().size(), 1U);
    const auto optimizedSink = optimizedSingleBranch.getRootOperators()[0];
    ASSERT_EQ(optimizedSink.getChildren().size(), 1U);
    const auto optimizedHigh = optimizedSink.getChildren()[0];
    const auto optimizedSource = leafOf(optimizedHigh);

    /// Second branch over the SAME optimizer-bound source operator, then both branches under one union. Traits are
    /// copied from the bound branch: nothing re-runs the optimizer after this point, so they have to be carried over.
    const auto low = LogicalOperator{SelectionLogicalOperator::create(optimizedSource, fieldCompare(optimizedSource, "value", "2", false))}
                         .withTraitSet(optimizedHigh.getTraitSet());
    const auto merged = LogicalOperator{UnionLogicalOperator::create({optimizedHigh, low})}.withTraitSet(optimizedHigh.getTraitSet());
    const auto sink
        = LogicalOperator{SinkLogicalOperator::create(merged, createFileSink("diamond_sink"))}.withTraitSet(optimizedSink.getTraitSet());
    auto plan = LogicalPlan{optimizedSingleBranch.getQueryId(), {sink}};

    ASSERT_EQ(countReachable(plan.getRootOperators()[0], optimizedSource.getId()), 2U)
        << "the shared source is not reachable from both branches, so this is not a diamond and the rest of this "
           "test would say nothing about sharing";

    const auto reached = runToTermination(std::move(plan));

    ASSERT_TRUE(reached.has_value()) << "the worker refused the plan, so it never got past compilation";
    EXPECT_EQ(reached.value(), QueryStatus::Stopped) << "the diamond did not deploy and run to end of stream";
}

}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
