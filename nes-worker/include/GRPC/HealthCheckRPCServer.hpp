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

#ifndef NES_WORKER_INCLUDE_GRPC_HEALTHCHECKRPCSERVER_HPP_
#define NES_WORKER_INCLUDE_GRPC_HEALTHCHECKRPCSERVER_HPP_
#include <map>
#include <mutex>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include <Health.grpc.pb.h>

namespace NES {
/**
 * This class usees the Heatlth service of GRPC to implement a keep alive service
 * This implementation follows the example: https://github.com/grpc/grpc/blob/master/test/cpp/end2end/test_health_check_service_impl.cc
 *
 */
class HealthCheckRPCServer : public grpc::health::v1::Health::Service {
  public:
    grpc::Status Check(grpc::ServerContext* context,
                       const grpc::health::v1::HealthCheckRequest* request,
                       grpc::health::v1::HealthCheckResponse* response) override;
    grpc::Status Watch(grpc::ServerContext* context,
                       const grpc::health::v1::HealthCheckRequest* request,
                       grpc::ServerWriter<grpc::health::v1::HealthCheckResponse>* writer) override;

    /**
    * Method to set the status for health checking for a particular service
    * @param serviceName
    * @param status
    */
    void SetStatus(const std::string& serviceName, grpc::health::v1::HealthCheckResponse::ServingStatus status);

    /**
    * Method to set the health checking for all services
    * @param status
    */
    void SetAll(grpc::health::v1::HealthCheckResponse::ServingStatus status);

    /**
    * Method to shutdown the health checking
    */
    void Shutdown();

  private:
    std::mutex mutex;
    bool shutdown = false;
    std::map<const std::string, grpc::health::v1::HealthCheckResponse::ServingStatus> statusMap;
};

}// namespace NES

#endif  // NES_WORKER_INCLUDE_GRPC_HEALTHCHECKRPCSERVER_HPP_
