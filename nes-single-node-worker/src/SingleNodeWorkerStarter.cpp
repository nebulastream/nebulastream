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

#include <csignal>


#include <Configurations/Util.hpp>
#include <grpcpp/server_builder.h>
#include <ErrorHandling.hpp>
#include <GrpcService.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

extern void init_receiver_server_string(std::string connection);
extern void init_sender_server();

int main(const int argc, const char* argv[])
{
    try
    {
        signal(SIGPIPE, SIG_IGN);
        NES::Logger::setupLogging("singleNodeWorker.log", NES::LogLevel::LOG_DEBUG);
        auto configuration = NES::Configurations::loadConfiguration<NES::Configuration::SingleNodeWorkerConfiguration>(argc, argv);
        if (!configuration)
        {
            return 0;
        }
        if (!configuration->dataUri.getValue().empty())
        {
            init_receiver_server_string(configuration->dataUri.getValue());
            init_sender_server();
        }

        NES::GRPCServer workerService{NES::SingleNodeWorker(*configuration)};

        grpc::ServerBuilder builder;
        builder.AddListeningPort(configuration->grpcAddressUri, grpc::InsecureServerCredentials());
        builder.RegisterService(&workerService);

        const auto server = builder.BuildAndStart();
        NES_INFO("Server listening on {}", static_cast<const std::string&>(configuration->grpcAddressUri));
        server->Wait();
        return 0;
    }
    catch (const NES::Exception& e)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentExceptionCode();
    }
    catch (...)
    {
        return NES::ErrorCode::UnknownException;
    }
}
