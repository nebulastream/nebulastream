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

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <queue>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <grpcpp/impl/service_type.h>
#include <grpcpp/server.h>

namespace NES
{

/// A request received over gRPC, waiting to be emitted as a row.
struct PendingStatisticRequest
{
    uint64_t statisticId;
    uint64_t startTs;
    uint64_t endTs;
};

/// Turns statistic interface-issued probe requests into a stream.
///
/// Hosts a gRPC server implementing StatisticSourceService; each RequestStatistic call enqueues one request,
/// which is emitted as a CSV row "statisticId,startTs,endTs". Downstream, a ScalarStatisticProbe turns that row
/// into a store lookup -- this is the impulse that drives the read path.
///
/// Rows are text rather than typed fields because that is this branch's source contract: a Source writes bytes
/// and the configured InputFormatter parses them (cf. InProcessSource). A query reading this source therefore
/// declares a three-column UINT64 schema and a CSV input formatter.
///
/// Like InProcessSource it never signals end-of-stream while running: an empty queue only means the statistic interface
/// has not asked for anything yet.
class GrpcSource final : public Source
{
public:
    constexpr static std::string_view NAME = "Grpc";

    explicit GrpcSource(const SourceDescriptor& sourceDescriptor);
    ~GrpcSource() override = default;

    GrpcSource(const GrpcSource&) = delete;
    GrpcSource& operator=(const GrpcSource&) = delete;
    GrpcSource(GrpcSource&&) = delete;
    GrpcSource& operator=(GrpcSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    /// Called by the gRPC service handler.
    void enqueueRequest(PendingStatisticRequest request);

    /// The port actually bound, which differs from the configured one when that was 0.
    [[nodiscard]] uint32_t getActualPort() const { return actualPort; }

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    uint32_t configuredPort;
    uint32_t actualPort{0};
    uint64_t flushIntervalMs;

    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<grpc::Service> service;

    std::mutex queueMutex;
    std::condition_variable queueCv;
    std::queue<PendingStatisticRequest> requestQueue;

    std::optional<std::string> pendingRow;
    size_t emittedRows{0};
};

struct ConfigParametersGrpcSource
{
    /// 0 lets the kernel choose; getActualPort() reports what was bound.
    static inline const DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "GRPC_PORT", 0, [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(PORT, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint64_t> FLUSH_INTERVAL_MS{
        "FLUSH_INTERVAL_MS",
        100,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto value = DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config);
            return value.has_value() && *value > 0 ? value : std::nullopt;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, PORT, FLUSH_INTERVAL_MS);
};

}
