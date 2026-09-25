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

/// A StatisticReportService that only records. The real one lives in the coordinator's `statistics` crate, which
/// a C++ test cannot stand up, so the tests that exercise GrpcSink report into this instead.

#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <vector>
#include <unistd.h>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <StatisticService.grpc.pb.h>
#include <StatisticService.pb.h>

namespace NES::StatisticTestSupport
{

class RecordingStatisticReportService final : public StatisticReportService::Service
{
public:
    grpc::Status ReportStatistics(grpc::ServerContext*, const StatisticReportBatch* batch, google::protobuf::Empty*) override
    {
        const std::lock_guard lock(mutex);
        reports.insert(reports.end(), batch->reports().begin(), batch->reports().end());
        return grpc::Status::OK;
    }

    [[nodiscard]] std::vector<StatisticReport> snapshot() const
    {
        const std::lock_guard lock(mutex);
        return reports;
    }

private:
    mutable std::mutex mutex;
    std::vector<StatisticReport> reports;
};

inline uint32_t pickFreePort()
{
    std::mt19937 generator{std::random_device{}()};
    std::uniform_int_distribution<uint32_t> candidates{20000, 31999};
    for (int attempt = 0; attempt < 50; ++attempt)
    {
        const uint32_t candidate = candidates(generator);
        const int socketFd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (socketFd < 0)
        {
            continue;
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_ANY);
        address.sin_port = htons(static_cast<uint16_t>(candidate));
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): the POSIX bind interface takes sockaddr.
        const bool bound = ::bind(socketFd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0;
        ::close(socketFd);
        if (bound)
        {
            return candidate;
        }
    }
    return 0;
}

/// Owns a service and the server hosting it, on a kernel-chosen port.
class TestStatisticServiceServer
{
public:
    TestStatisticServiceServer()
    {
        grpc::ServerBuilder builder;
        int selected = 0;
        builder.AddListeningPort("0.0.0.0:0", grpc::InsecureServerCredentials(), &selected);
        builder.RegisterService(&service);
        server = builder.BuildAndStart();
        port = static_cast<uint32_t>(selected);
    }

    ~TestStatisticServiceServer()
    {
        if (server)
        {
            server->Shutdown();
        }
    }

    TestStatisticServiceServer(const TestStatisticServiceServer&) = delete;
    TestStatisticServiceServer& operator=(const TestStatisticServiceServer&) = delete;
    TestStatisticServiceServer(TestStatisticServiceServer&&) = delete;
    TestStatisticServiceServer& operator=(TestStatisticServiceServer&&) = delete;

    [[nodiscard]] uint32_t getPort() const { return port; }

    [[nodiscard]] bool isRunning() const { return server != nullptr; }

    [[nodiscard]] std::vector<StatisticReport> reports() const { return service.snapshot(); }

private:
    RecordingStatisticReportService service;
    std::unique_ptr<grpc::Server> server;
    uint32_t port{0};
};

}
