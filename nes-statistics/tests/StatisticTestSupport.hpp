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

#pragma once

/// Scaffolding shared by the statistic tests that actually compile and run queries.
///
/// There is no upstream harness to borrow: LogicalPlanBuilder has exactly one caller (the SQL parser) and no
/// existing test runs a programmatically built plan. Plans are therefore assembled by hand, using the anonymous
/// source and sink overloads so that no catalog registration is needed.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <DistributedLogicalPlan.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <ModelCatalog.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Statistic/ScalarStatisticProbeLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/ScalarStatisticAggregationLogicalFunction.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <QueryStatus.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <gtest/gtest.h>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES::StatisticTestSupport
{

inline const Host TEST_HOST{"localhost"};
inline constexpr uint64_t STATISTIC_ID = 401;
inline constexpr uint64_t WINDOW_SIZE_MS = 1000;

/// Two windows of four tuples each, so the averages are exact in FLOAT64 and distinguishable from one another:
/// [0, 1000) averages 20, [1000, 2000) averages 200.
inline constexpr std::string_view INPUT_CSV = "100,10\n200,20\n300,25\n400,25\n"
                                              "1100,100\n1200,200\n1300,250\n1400,250\n";

inline std::filesystem::path writeInput(const std::string& name)
{
    const auto path = std::filesystem::temp_directory_path() / name;
    std::ofstream out{path};
    out << INPUT_CSV;
    out.close();
    return path;
}

inline std::string readFile(const std::filesystem::path& path)
{
    const std::ifstream in{path};
    std::ostringstream contents;
    contents << in.rdbuf();
    return contents.str();
}

inline Schema<UnqualifiedUnboundField, Ordered> inputSchema()
{
    const auto uint64Type = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
    return Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse("ts"), uint64Type}, UnqualifiedUnboundField{Identifier::parse("value"), uint64Type}};
}

/// Source -> watermark -> windowed aggregation carrying a ScalarStatistic. The writer is fused into the
/// aggregation's lowering, so nothing about it appears here, and addWindowAggregation inserts the watermark
/// assigner itself.
inline LogicalPlan buildStatisticPlan(const std::filesystem::path& inputPath)
{
    auto plan = LogicalPlanBuilder::createLogicalPlan(
        Identifier::parse("File"),
        inputSchema(),
        {{Identifier::parse("FILE_PATH"), inputPath.string()}, {Identifier::parse("host"), std::string{TEST_HOST.getRawValue()}}},
        {{Identifier::parse("type"), "CSV"}});

    const Windowing::TimeBasedWindowType windowType{Windowing::TumblingWindow{Windowing::TimeMeasure{WINDOW_SIZE_MS}}};
    const Windowing::TimeCharacteristic timeCharacteristic{
        Windowing::UnboundTimeCharacteristic{Windowing::TimeCharacteristicWrapper::createEventTime(
            UnboundFieldAccessLogicalFunction{Identifier::parse("ts")}, Windowing::TimeUnit::Milliseconds())}};

    const ScalarStatisticAggregationLogicalFunction statisticFunction{
        TypedLogicalFunction<UnboundFieldAccessLogicalFunction>{UnboundFieldAccessLogicalFunction{Identifier::parse("value")}},
        StatisticId{STATISTIC_ID},
        StatisticType::Avg};

    return LogicalPlanBuilder::addWindowAggregation(
        plan,
        windowType,
        {WindowedAggregationLogicalOperator::ProjectedAggregation{statisticFunction, statisticDataFieldName(StatisticId{STATISTIC_ID})}},
        {},
        timeCharacteristic);
}

/// Chains a probe onto a build plan.
///
/// Built childless and attached with withChildrenUnsafe, mirroring LogicalPlanBuilder::promoteOperatorToRoot.
/// The child-taking constructor infers eagerly, but nothing in a freshly built plan has been inferred yet --
/// that is the optimizer's TypeInferenceRule -- so inferring here would read an unset schema off the child.
inline LogicalPlan addScalarProbe(
    const LogicalPlan& plan, const Identifier& startField = Identifier::parse("start"),
    const Identifier& endField = Identifier::parse("end"))
{
    const auto probe = ScalarStatisticProbeLogicalOperator::create(
        StatisticId{STATISTIC_ID},
        StatisticType::Avg,
        DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE),
        startField,
        endField);
    return plan.withRootOperators({LogicalOperator{probe}.withChildrenUnsafe(plan.getRootOperators())});
}

inline LogicalPlan addFileSink(const LogicalPlan& plan, const std::filesystem::path& outputPath)
{
    return LogicalPlanBuilder::addAnonymousSink(
        Identifier::parse("File"),
        std::nullopt,
        {{Identifier::parse("FILE_PATH"), outputPath.string()},
         {Identifier::parse("OUTPUT_FORMAT"), "CSV"},
         {Identifier::parse("host"), std::string{TEST_HOST.getRawValue()}}},
        {},
        plan);
}

inline LogicalPlan addGrpcSink(const LogicalPlan& plan, const uint32_t port)
{
    return LogicalPlanBuilder::addAnonymousSink(
        Identifier::parse("Grpc"),
        std::nullopt,
        {{Identifier::parse("grpc_host"), "localhost"},
         {Identifier::parse("grpc_port"), std::to_string(port)},
         {Identifier::parse("OUTPUT_FORMAT"), "CSV"},
         {Identifier::parse("host"), std::string{TEST_HOST.getRawValue()}}},
        {},
        plan);
}

/// Runs the optimizer and returns the single local plan a single-node setup produces.
///
/// SingleNodeWorker::startQuery compiles the plan as given; it does not optimize. The optimizer is what resolves
/// the anonymous sink into a concrete one and stamps the traits the lowering rules read (MemoryLayoutTypeTrait,
/// FieldMappingTrait, OutputOriginIdsTrait), so it has to run first.
inline std::optional<LogicalPlan> optimizeToLocalPlan(const LogicalPlan& plan)
{
    const auto sourceCatalog = std::make_shared<SourceCatalog>();
    const auto sinkCatalog = std::make_shared<SinkCatalog>();
    /// Operator placement needs somewhere to put the operators, and the source and sink carry a matching `host`.
    const auto workerCatalog = std::make_shared<WorkerCatalog>();
    workerCatalog->addWorker(TEST_HOST, "localhost:0", Capacity{CapacityKind::Unlimited{}}, {});
    const auto modelCatalog = std::make_shared<ModelCatalog>();

    const QueryOptimizer optimizer{QueryOptimizerConfiguration{}, sourceCatalog, sinkCatalog, workerCatalog, modelCatalog};
    const auto distributedPlan = optimizer.optimize(plan);
    if (distributedPlan.size() != 1)
    {
        return std::nullopt;
    }
    const auto& localPlans = distributedPlan.begin()->second;
    if (localPlans.size() != 1)
    {
        return std::nullopt;
    }
    return localPlans.front();
}

/// Optimizes a plan and runs it to completion on a single-node worker. Only usable with a terminating source.
inline void runToCompletion(const LogicalPlan& plan)
{
    const auto localPlan = optimizeToLocalPlan(plan);
    ASSERT_TRUE(localPlan.has_value()) << "single-node test expects exactly one local plan";

    const SingleNodeWorkerConfiguration configuration;
    SingleNodeWorker worker{configuration};
    const auto queryId = worker.startQuery(localPlan.value());
    ASSERT_TRUE(queryId.has_value()) << queryId.error().what();

    /// The File source terminates, so the query reaches a terminal state on its own.
    for (int attempt = 0; attempt < 200; ++attempt)
    {
        const auto status = worker.getQueryStatus(queryId.value());
        if (status.has_value() and status.value().state == QueryStatus::Stopped)
        {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }
    FAIL() << "query did not reach a terminal state";
}

}
