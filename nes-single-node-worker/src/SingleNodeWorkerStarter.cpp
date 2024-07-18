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
#include <Configurations/BaseConfiguration.hpp>
#include <grpcpp/server_builder.h>
#include <Configuration.hpp>
#include <GrpcService.hpp>
#include <SingleNodeWorker.hpp>

int main(const int argc, const char* argv[])
{
    NES::Logger::setupLogging("singleNodeWorker.log", NES::LogLevel::LOG_DEBUG);
    auto configuration = NES::Configuration::loadConfiguration<NES::Configuration::SingleNodeWorkerConfiguration>(argc, argv);
    NES::GRPCServer workerService{NES::SingleNodeWorker(configuration)};

    grpc::ServerBuilder builder;
    builder.AddListeningPort(configuration.grpcAddressUri, grpc::InsecureServerCredentials());
    builder.RegisterService(&workerService);

    const auto server = builder.BuildAndStart();
    NES_INFO("Server listening on {}", static_cast<const std::string&>(configuration.grpcAddressUri));
    server->Wait();
}
