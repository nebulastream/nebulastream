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

#include <StatisticInterface.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/ScalarStatisticProbeLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Schema/Schema.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <ErrorHandling.hpp>
#include <StatisticService.grpc.pb.h>
#include <StatisticService.pb.h>

namespace NES
{

namespace
{

/// Bound to a kernel-chosen port; getInterfaceAddress reports what was selected.
constexpr auto LISTEN_ADDRESS = "0.0.0.0:0";

/// Ports the probe queries try in turn. A probe deploys its own GrpcSource, so it needs a free one.
constexpr uint32_t PROBE_SOURCE_PORT_BASE = 10000;
constexpr uint32_t PROBE_SOURCE_PORT_ATTEMPTS = 10;

/// Routes incoming reports to the statistic interface.
class StatisticInterfaceServiceImpl final : public StatisticInterfaceService::Service
{
public:
    explicit StatisticInterfaceServiceImpl(StatisticInterface& statisticInterface) : statisticInterface(statisticInterface) { }

    grpc::Status ReportStatistic(grpc::ServerContext*, const StatisticReport* report, google::protobuf::Empty*) override
    {
        statisticInterface.onStatisticReport(
            StatisticTuple::StatisticId{report->statistic_id()},
            Windowing::TimeMeasure{report->start_ts()},
            Windowing::TimeMeasure{report->end_ts()},
            report->value());
        return grpc::Status::OK;
    }

private:
    StatisticInterface& statisticInterface;
};

/// The three columns a GrpcSource emits, which is what a probe consumes.
Schema<UnqualifiedUnboundField, Ordered> probeSourceSchema()
{
    const auto uint64Type = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
    return Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse(std::string{StatisticFieldNames::STATISTIC_ID}), uint64Type},
        UnqualifiedUnboundField{Identifier::parse(std::string{StatisticFieldNames::START_TS}), uint64Type},
        UnqualifiedUnboundField{Identifier::parse(std::string{StatisticFieldNames::END_TS}), uint64Type}};
}

std::pair<std::string, std::string> splitAddress(const std::string& address)
{
    const auto colon = address.rfind(':');
    if (colon == std::string::npos)
    {
        throw InvalidConfigParameter("StatisticTuple interface address '{}' is not in host:port form", address);
    }
    return {address.substr(0, colon), address.substr(colon + 1)};
}

}

StatisticInterface::StatisticInterface(std::unique_ptr<StatisticQueryGenerator> queryGenerator, SubmitQueryFn submitQuery)
    : queryGenerator(std::move(queryGenerator)), submitQuery(std::move(submitQuery))
{
}

StatisticInterface::~StatisticInterface()
{
    stopGrpcServer();
}

std::expected<CollectStatisticResult, Exception> StatisticInterface::collectNewStatistic(const RequestStatisticBuildStatement& statement)
{
    const StatisticRegistry::Key key{
        .metric = statement.metric, .collectionDomain = statement.domain, .windowSize = statement.windowType.getSize()};

    if (const auto existing = registry.find(key))
    {
        /// The deployed query's shape was fixed by the request that created it, and there is no redeployment
        /// here: a trigger added to an entry whose query terminates in a VoidSink can never fire, because that
        /// query reports nothing. Registering it anyway keeps the record of what was asked for.
        registry.addTrigger(key, statement.conditionTrigger);
        return CollectStatisticResult{.queryId = existing->queryId, .statisticId = existing->statisticId, .alreadyExisted = true};
    }

    const auto statisticId = StatisticTuple::StatisticId{nextStatisticId.fetch_add(1)};
    if (interfaceAddress.empty())
    {
        return std::unexpected(
            InvalidConfigParameter("startGrpcServer() has to run before a statistic can be collected: the sink needs an address"));
    }

    /// generateQuery throws for the domains and metrics outside this port's slice; surface that as the error
    /// channel the rest of the API uses rather than letting it escape.
    try
    {
        return submitQuery(queryGenerator->generateQuery(statement, statisticId, interfaceAddress))
            .transform(
                [this, &key, statisticId, &statement](auto queryId)
                {
                    registry.registerStatistic(key, queryId, statisticId, {statement.conditionTrigger});
                    return CollectStatisticResult{.queryId = queryId, .statisticId = statisticId, .alreadyExisted = false};
                });
    }
    catch (const Exception& e)
    {
        return std::unexpected(e);
    }
}

bool StatisticInterface::addConditionTrigger(const StatisticRegistry::Key& key, ConditionTrigger trigger)
{
    return registry.addTrigger(key, std::move(trigger));
}

bool StatisticInterface::deregisterStatistic(const StatisticRegistry::Key& key)
{
    return registry.deregisterStatistic(key);
}

std::string StatisticInterface::startGrpcServer()
{
    auto service = std::make_shared<StatisticInterfaceServiceImpl>(*this);
    grpc::ServerBuilder builder;
    int selectedPort = 0;
    builder.AddListeningPort(LISTEN_ADDRESS, grpc::InsecureServerCredentials(), &selectedPort);
    builder.RegisterService(service.get());
    grpcServer = builder.BuildAndStart();
    if (not grpcServer)
    {
        throw QueryStartFailed("StatisticInterface: failed to start its gRPC server");
    }
    /// Kept alive alongside the server, which holds a bare pointer to it.
    grpcService = std::move(service);
    interfaceAddress = "localhost:" + std::to_string(selectedPort);
    NES_INFO("StatisticInterface gRPC server listening on {}", interfaceAddress);
    return interfaceAddress;
}

void StatisticInterface::stopGrpcServer()
{
    if (grpcServer)
    {
        grpcServer->Shutdown();
        grpcServer.reset();
        NES_DEBUG("StatisticInterface gRPC server stopped.");
    }
    grpcService.reset();
}

std::optional<double> StatisticInterface::getStatistics(
    const std::vector<StatisticRegistry::Key>& keys, const Windowing::TimeMeasure startTs, const Windowing::TimeMeasure endTs)
{
    if (interfaceAddress.empty())
    {
        throw InvalidConfigParameter("startGrpcServer() has to run before statistics can be probed");
    }

    std::vector<StatisticTuple::StatisticId> statisticIds;
    statisticIds.reserve(keys.size());
    for (const auto& key : keys)
    {
        const auto entry = registry.find(key);
        if (not entry.has_value())
        {
            throw QueryNotFound("StatisticInterface::getStatistics: key not found in the registry");
        }
        statisticIds.push_back(entry->statisticId);
    }

    const auto [sinkHost, sinkPort] = splitAddress(interfaceAddress);

    /// Deploy the probe query. Its source needs a free port, and nothing reports which ones are taken, so this
    /// walks a small range until one binds.
    uint32_t sourcePort = 0;
    bool deployed = false;
    for (uint32_t attempt = 0; attempt < PROBE_SOURCE_PORT_ATTEMPTS and not deployed; ++attempt)
    {
        sourcePort = PROBE_SOURCE_PORT_BASE + attempt;

        auto plan = LogicalPlanBuilder::createLogicalPlan(
            Identifier::parse("Grpc"),
            probeSourceSchema(),
            {{Identifier::parse("grpc_port"), std::to_string(sourcePort)}, {Identifier::parse("host"), sinkHost}},
            {{Identifier::parse("type"), "CSV"}});

        /// One probe operator per statistic, chained, so a single query serves every requested key. Each is
        /// built childless and attached with withChildrenUnsafe: schema inference is the optimizer's job, and
        /// the child-taking constructor would infer against a plan that has not been inferred yet.
        for (const auto& statisticId : statisticIds)
        {
            const auto probe = ScalarStatisticProbeLogicalOperator::create(
                statisticId,
                StatisticType::Avg,
                DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE),
                Identifier::parse(std::string{StatisticFieldNames::START_TS}),
                Identifier::parse(std::string{StatisticFieldNames::END_TS}));
            plan = plan.withRootOperators({LogicalOperator{probe}.withChildrenUnsafe(plan.getRootOperators())});
        }

        plan = LogicalPlanBuilder::addAnonymousSink(
            Identifier::parse("Grpc"),
            std::nullopt,
            {{Identifier::parse("grpc_host"), sinkHost},
             {Identifier::parse("grpc_port"), sinkPort},
             {Identifier::parse("OUTPUT_FORMAT"), "CSV"},
             {Identifier::parse("host"), sinkHost}},
            {},
            plan);

        if (auto submitted = submitQuery(std::move(plan)); submitted.has_value())
        {
            deployed = true;
            NES_DEBUG("StatisticInterface::getStatistics: probe query deployed with source port {}", sourcePort);
        }
        else
        {
            NES_WARNING("StatisticInterface::getStatistics: probe query on port {} failed: {}", sourcePort, submitted.error().what());
        }
    }
    if (not deployed)
    {
        throw QueryStartFailed("StatisticInterface::getStatistics: no probe query could be deployed");
    }

    /// Register the pending entries before the impulses go out, so a fast report cannot arrive unclaimed.
    {
        const std::lock_guard lock(probeMutex);
        for (const auto& statisticId : statisticIds)
        {
            pendingProbes[statisticId] = PendingProbe{};
        }
    }

    /// Trigger the probes. The source binds during open(), which happens after the query is submitted, so this
    /// retries until the first call is accepted.
    auto channel = grpc::CreateChannel("localhost:" + std::to_string(sourcePort), grpc::InsecureChannelCredentials());
    auto sourceStub = StatisticSourceService::NewStub(channel);
    for (const auto& statisticId : statisticIds)
    {
        StatisticRequest request;
        request.set_statistic_id(statisticId.getRawValue());
        request.set_start_ts(startTs.getTime());
        request.set_end_ts(endTs.getTime());

        bool delivered = false;
        const auto deadline = std::chrono::steady_clock::now() + PROBE_TIMEOUT;
        while (not delivered and std::chrono::steady_clock::now() < deadline)
        {
            grpc::ClientContext context;
            google::protobuf::Empty response;
            delivered = sourceStub->RequestStatistic(&context, request, &response).ok();
            if (not delivered)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds{50});
            }
        }
        if (not delivered)
        {
            NES_WARNING("StatisticInterface::getStatistics: the probe source never accepted a request for {}", statisticId);
        }
    }

    /// Wait until every probed statistic has reported at least once, then let stragglers land: a range probe
    /// emits one row per stored window and the count is not known up front.
    bool allReported = false;
    {
        std::unique_lock lock(probeMutex);
        allReported = probeCv.wait_for(
            lock,
            PROBE_TIMEOUT,
            [&] { return std::ranges::all_of(statisticIds, [&](const auto& id) { return pendingProbes[id].reports > 0; }); });
    }
    if (allReported)
    {
        std::this_thread::sleep_for(PROBE_SETTLE_INTERVAL);
    }

    double result = 0;
    {
        const std::lock_guard lock(probeMutex);
        for (const auto& statisticId : statisticIds)
        {
            const auto it = pendingProbes.find(statisticId);
            if (it != pendingProbes.end())
            {
                result += it->second.sum;
                if (it->second.reports == 0)
                {
                    NES_WARNING("StatisticInterface::getStatistics: no report arrived for {}", statisticId);
                }
                pendingProbes.erase(it);
            }
        }
    }

    return allReported ? std::optional{result} : std::nullopt;
}

void StatisticInterface::onStatisticReport(
    const StatisticTuple::StatisticId statisticId,
    const Windowing::TimeMeasure startTs,
    const Windowing::TimeMeasure endTs,
    const double value)
{
    /// A pending probe takes precedence: it is a caller actively blocked on this id. The entry is left in place
    /// and accumulated into rather than erased, because one probe can report several windows.
    {
        const std::lock_guard lock(probeMutex);
        if (const auto it = pendingProbes.find(statisticId); it != pendingProbes.end())
        {
            it->second.sum += value;
            ++it->second.reports;
            probeCv.notify_all();
            return;
        }
    }

    /// Otherwise this is a build query reporting a closed window; fire any triggers registered for it.
    registry.forEachEntry(
        [&](const StatisticRegistry::Key&, const StatisticRegistry::Entry& entry)
        {
            if (entry.statisticId != statisticId)
            {
                return;
            }
            for (const auto& trigger : entry.triggers)
            {
                if (trigger.callback)
                {
                    trigger.callback(statisticId, startTs, endTs, value);
                }
            }
        });
}

}
