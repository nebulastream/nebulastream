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

#include <GrpcSource.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <StatisticService.grpc.pb.h>
#include <StatisticService.pb.h>

#include <Sources/RowBufferPacking.hpp>

namespace NES
{

namespace
{

/// Bridges the gRPC service onto the owning source. Held by the source, so it outlives the server.
class StatisticSourceServiceImpl final : public StatisticSourceService::Service
{
public:
    explicit StatisticSourceServiceImpl(GrpcSource& source) : source(source) { }

    grpc::Status RequestStatistic(grpc::ServerContext*, const StatisticRequest* request, google::protobuf::Empty*) override
    {
        source.enqueueRequest(
            PendingStatisticRequest{.statisticId = request->statistic_id(), .startTs = request->start_ts(), .endTs = request->end_ts()});
        return grpc::Status::OK;
    }

private:
    GrpcSource& source;
};

}

GrpcSource::GrpcSource(const SourceDescriptor& sourceDescriptor)
    : port(sourceDescriptor.getFromConfig(ConfigParametersGrpcSource::PORT))
    , flushIntervalMs(sourceDescriptor.getFromConfig(EventFeedSourceConfig::FLUSH_INTERVAL_MS))
{
}

void GrpcSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    auto impl = std::make_unique<StatisticSourceServiceImpl>(*this);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(fmt::format("0.0.0.0:{}", port), grpc::InsecureServerCredentials());
    builder.RegisterService(impl.get());
    grpcServer = builder.BuildAndStart();
    if (not grpcServer)
    {
        throw CannotOpenSource("GrpcSource: failed to start a gRPC server on port {}", port);
    }
    service = std::move(impl);
    NES_INFO("GrpcSource is listening on port {}", port);
}

void GrpcSource::close()
{
    if (grpcServer)
    {
        grpcServer->Shutdown();
        grpcServer.reset();
    }
    service.reset();
    /// Wake anything still blocked on an empty queue so it observes the stop token.
    queueCv.notify_all();
    NES_INFO("Closing GrpcSource after {} rows", emittedRows);
}

void GrpcSource::enqueueRequest(PendingStatisticRequest request)
{
    {
        const std::lock_guard lock(queueMutex);
        requestQueue.push(request);
    }
    queueCv.notify_one();
}

Source::FillTupleBufferResult GrpcSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    return packRowsIntoBuffer(
        tupleBuffer,
        stopToken,
        std::chrono::milliseconds{flushIntervalMs},
        pendingRow,
        emittedRows,
        [this](const std::chrono::milliseconds timeout) -> std::optional<std::string>
        {
            std::unique_lock lock(queueMutex);
            queueCv.wait_for(lock, timeout, [this] { return not requestQueue.empty(); });
            if (requestQueue.empty())
            {
                return std::nullopt;
            }
            const auto request = requestQueue.front();
            requestQueue.pop();
            lock.unlock();
            return fmt::format("{},{},{}", request.statisticId, request.startTs, request.endTs);
        },
        fmt::format("the gRPC statistic source on port {}", port));
}

std::ostream& GrpcSource::toString(std::ostream& str) const
{
    return str << fmt::format("GrpcSource(port: {})", port);
}

DescriptorConfig::Config GrpcSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersGrpcSource>(std::move(config), NAME);
}

}
