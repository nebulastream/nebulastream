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

#include <Configurations/Util.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <cpptrace/from_current.hpp>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <ErrorHandling.hpp>
#include <GrpcService.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

int main(const int argc, const char* argv[])
{
    CPPTRACE_TRY
    {
        NES::Logger::setupLogging("singleNodeWorker.log", NES::LogLevel::LOG_DEBUG);
        auto configuration = NES::Configurations::loadConfiguration<NES::Configuration::SingleNodeWorkerConfiguration>(argc, argv);
        if (!configuration)
        {
            return 0;
        }

        NES::GRPCServer workerService{NES::SingleNodeWorker(*configuration)};
        grpc::EnableDefaultHealthCheckService(true);

        grpc::ServerBuilder builder;
        builder.AddListeningPort(configuration->grpcAddressUri, grpc::InsecureServerCredentials());
        builder.RegisterService(&workerService);

        const auto server = builder.BuildAndStart();
        NES_INFO("Server listening on {}", static_cast<const std::string&>(configuration->grpcAddressUri));
        server->Wait();
        return 0;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
