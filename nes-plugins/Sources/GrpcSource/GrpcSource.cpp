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
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <StatisticService.grpc.pb.h>
#include <StatisticService.pb.h>

namespace NES
{

namespace
{

/// Matches the tuple delimiter the InputFormatter expects between rows.
constexpr char TUPLE_DELIMITER = '\n';

/// How long a single queue wait blocks before re-checking the stop token.
constexpr auto POP_TIMEOUT = std::chrono::milliseconds{50};

/// Bridges the gRPC service onto the owning source. Held by the source, so it outlives the server.
class StatisticSourceServiceImpl final : public StatisticSourceService::Service
{
public:
    explicit StatisticSourceServiceImpl(GrpcSource& source) : source(source) { }

    grpc::Status RequestStatistic(grpc::ServerContext*, const StatisticRequest* request, google::protobuf::Empty*) override
    {
        source.enqueueRequest(
            PendingStatisticRequest{
                .statisticId = request->statistic_id(), .startTs = request->start_ts(), .endTs = request->end_ts()});
        return grpc::Status::OK;
    }

private:
    GrpcSource& source;
};

}

GrpcSource::GrpcSource(const SourceDescriptor& sourceDescriptor)
    : configuredPort(sourceDescriptor.getFromConfig(ConfigParametersGrpcSource::PORT))
    , flushIntervalMs(sourceDescriptor.getFromConfig(ConfigParametersGrpcSource::FLUSH_INTERVAL_MS))
{
}

void GrpcSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    auto impl = std::make_unique<StatisticSourceServiceImpl>(*this);
    grpc::ServerBuilder builder;
    int selectedPort = 0;
    builder.AddListeningPort(fmt::format("0.0.0.0:{}", configuredPort), grpc::InsecureServerCredentials(), &selectedPort);
    builder.RegisterService(impl.get());
    grpcServer = builder.BuildAndStart();
    if (not grpcServer)
    {
        throw CannotOpenSource("GrpcSource: failed to start a gRPC server on port {}", configuredPort);
    }
    service = std::move(impl);
    actualPort = static_cast<uint32_t>(selectedPort);
    NES_INFO("GrpcSource is listening on port {}", actualPort);
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
    const auto available = tupleBuffer.getAvailableMemoryArea<char>();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{flushIntervalMs};
    size_t written = 0;

    while (not stopToken.stop_requested())
    {
        if (not pendingRow.has_value())
        {
            std::unique_lock lock(queueMutex);
            queueCv.wait_for(lock, POP_TIMEOUT, [this] { return not requestQueue.empty(); });
            if (not requestQueue.empty())
            {
                const auto request = requestQueue.front();
                requestQueue.pop();
                lock.unlock();
                pendingRow = fmt::format("{},{},{}", request.statisticId, request.startTs, request.endTs);
            }
        }

        if (pendingRow.has_value())
        {
            /// The delimiter is part of what the InputFormatter has to see, so it counts towards the row size.
            if (const size_t rowSize = pendingRow->size() + 1; rowSize <= available.size() - written)
            {
                std::ranges::copy(*pendingRow, std::next(available.begin(), static_cast<ptrdiff_t>(written)));
                written += pendingRow->size();
                available[written++] = TUPLE_DELIMITER;
                ++emittedRows;
                pendingRow.reset();
            }
            else if (written > 0)
            {
                /// Keep the row for the next buffer.
                break;
            }
            else
            {
                NES_WARNING("Dropping a probe request of {} bytes, it does not fit into a TupleBuffer", pendingRow->size() + 1);
                pendingRow.reset();
            }
        }

        /// Returning zero bytes would terminate the source, so an empty queue just means we keep waiting.
        if (written > 0 && std::chrono::steady_clock::now() >= deadline)
        {
            break;
        }
    }

    if (written == 0)
    {
        return FillTupleBufferResult::eos();
    }
    return FillTupleBufferResult::withBytes(written);
}

std::ostream& GrpcSource::toString(std::ostream& str) const
{
    return str << fmt::format("GrpcSource(port: {})", actualPort != 0 ? actualPort : configuredPort);
}

DescriptorConfig::Config GrpcSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersGrpcSource>(std::move(config), NAME);
}

}
