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

#include <GrpcSystemStatsReader.hpp>

#include <chrono>
#include <string>
#include <utility>

#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <Util/Logger/Logger.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES::Systest
{

GrpcSystemStatsReader::GrpcSystemStatsReader(SystemStatsBroadcaster* broadcaster)
    : broadcaster(broadcaster)
{
    embeddedSubscriber = broadcaster->subscribe();
    threads.emplace_back(&GrpcSystemStatsReader::embeddedReaderThread, this, embeddedSubscriber);
}

GrpcSystemStatsReader::GrpcSystemStatsReader(const std::vector<std::string>& grpcAddresses)
{
    for (const auto& addr : grpcAddresses)
    {
        threads.emplace_back(&GrpcSystemStatsReader::grpcReaderThread, this, addr);
    }
}

GrpcSystemStatsReader::~GrpcSystemStatsReader()
{
    stop();
}

void GrpcSystemStatsReader::stop()
{
    running.store(false);
    cv.notify_all();
    if (embeddedSubscriber)
    {
        embeddedSubscriber->close();
    }
    for (auto& t : threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }
    threads.clear();
    if (broadcaster && embeddedSubscriber)
    {
        broadcaster->unsubscribe(embeddedSubscriber);
        embeddedSubscriber.reset();
    }
}

void GrpcSystemStatsReader::processEvent(const SystemStatEvent& event)
{
    if (event.has_query_status())
    {
        const auto& qs = event.query_status();
        if (qs.status() == "stopped" || qs.status() == "failed")
        {
            std::lock_guard lock(statusMutex);
            terminalEvents.push(QueryStatusEvent{qs.timestamp_ms(), qs.query_id(), qs.status()});
            cv.notify_all();
        }
    }
    else if (event.has_pipeline_compiled())
    {
        const auto& pc = event.pipeline_compiled();
        std::lock_guard lock(compilationMutex);
        auto& stats = compilationStatsPerQuery[pc.query_id()];
        stats.totalCompileTimeNs += pc.compile_time_ns();
        stats.pipelineCount++;
    }
    else if (event.has_buffer_ingestion())
    {
        const auto& bi = event.buffer_ingestion();
        std::lock_guard lock(ingestionMutex);
        auto& stats = ingestionStatsPerQuery[bi.query_id()];
        stats.totalTuples += bi.total_tuples();
        stats.totalBuffers += bi.ingestion_count();
        stats.windowCount++;
    }
}

void GrpcSystemStatsReader::embeddedReaderThread(std::shared_ptr<SystemStatsSubscriber> subscriber)
{
    while (running.load())
    {
        SystemStatEvent event;
        if (subscriber->tryPopUntil(std::chrono::steady_clock::now() + std::chrono::seconds(1), event))
        {
            processEvent(event);
        }
    }
}

void GrpcSystemStatsReader::grpcReaderThread(std::string grpcAddress)
{
    auto channel = grpc::CreateChannel(grpcAddress, grpc::InsecureChannelCredentials());
    auto stub = WorkerRPCService::NewStub(channel);

    while (running.load())
    {
        grpc::ClientContext context;
        SystemStatsRequest request;
        auto reader = stub->StreamSystemStats(&context, request);

        SystemStatEvent event;
        while (reader->Read(&event))
        {
            if (!running.load())
            {
                break;
            }
            processEvent(event);
        }

        auto status = reader->Finish();
        if (!running.load())
        {
            break;
        }

        // Reconnect after a brief pause if stream ended
        NES_DEBUG("gRPC stats stream to {} ended ({}), reconnecting...", grpcAddress, status.error_message());
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

std::vector<QueryStatusEvent> GrpcSystemStatsReader::waitForTerminalEvents()
{
    std::unique_lock lock(statusMutex);
    cv.wait(lock, [this] { return !terminalEvents.empty() || !running.load(); });

    std::vector<QueryStatusEvent> result;
    while (!terminalEvents.empty())
    {
        result.push_back(std::move(terminalEvents.front()));
        terminalEvents.pop();
    }
    return result;
}

CompilationStats GrpcSystemStatsReader::getCompilationStats(const std::string& localQueryId) const
{
    std::lock_guard lock(compilationMutex);
    auto it = compilationStatsPerQuery.find(localQueryId);
    if (it != compilationStatsPerQuery.end())
    {
        return it->second;
    }
    return {};
}

IngestionStats GrpcSystemStatsReader::getIngestionStats(const std::string& localQueryId) const
{
    std::lock_guard lock(ingestionMutex);
    auto it = ingestionStatsPerQuery.find(localQueryId);
    if (it != ingestionStatsPerQuery.end())
    {
        return it->second;
    }
    return {};
}

}
