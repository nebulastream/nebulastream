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

#include <GRPC/HealthCheckRPCServer.hpp>
#include <grpc/grpc.h>

using grpc::health::v1::HealthCheckRequest;
using grpc::health::v1::HealthCheckResponse;

namespace NES {

grpc::Status NES::HealthCheckRPCServer::Check(grpc::ServerContext* /*context*/,
                                              const HealthCheckRequest* request,
                                              HealthCheckResponse* response) {
    std::lock_guard<std::mutex> lock(mutex);
    auto iter = statusMap.find(request->service());
    if (iter == statusMap.end()) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "");
    }
    response->set_status(iter->second);
    return grpc::Status::OK;
}

grpc::Status HealthCheckRPCServer::Watch(grpc::ServerContext* context,
                                         const HealthCheckRequest* request,
                                         grpc::ServerWriter<HealthCheckResponse>* writer) {
    auto last_state = HealthCheckResponse::UNKNOWN;
    while (!context->IsCancelled()) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            HealthCheckResponse response;
            auto iter = statusMap.find(request->service());
            if (iter == statusMap.end()) {
                response.set_status(response.SERVICE_UNKNOWN);
            } else {
                response.set_status(iter->second);
            }
            if (response.status() != last_state) {
                writer->Write(response, grpc::WriteOptions());
                last_state = response.status();
            }
        }
        gpr_sleep_until(gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC), gpr_time_from_millis(1000, GPR_TIMESPAN)));
    }
    return grpc::Status::OK;
}

void HealthCheckRPCServer::SetStatus(const std::string& service_name,
                                     grpc::health::v1::HealthCheckResponse::ServingStatus status) {
    std::lock_guard<std::mutex> lock(mutex);
    if (shutdown) {
        status = HealthCheckResponse::NOT_SERVING;
    }
    statusMap[service_name] = status;
}

void HealthCheckRPCServer::SetAll(grpc::health::v1::HealthCheckResponse::ServingStatus status) {
    std::lock_guard<std::mutex> lock(mutex);
    if (shutdown) {
        return;
    }
    for (auto iter = statusMap.begin(); iter != statusMap.end(); ++iter) {
        iter->second = status;
    }
}

void HealthCheckRPCServer::Shutdown() {
    std::lock_guard<std::mutex> lock(mutex);
    if (shutdown) {
        return;
    }
    shutdown = true;
    for (auto iter = statusMap.begin(); iter != statusMap.end(); ++iter) {
        iter->second = HealthCheckResponse::NOT_SERVING;
    }
}

}// namespace NES
