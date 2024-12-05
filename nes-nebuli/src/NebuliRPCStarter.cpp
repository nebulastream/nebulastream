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

#include <API/AttributeField.hpp>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Util.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <grpcpp/create_channel.h>
#include <grpcpp/server_builder.h>

#include <GRPCClient.hpp>
#include <NebuLI.hpp>
#include <NebuliRPCService.grpc.pb.h>

namespace NES
{
struct NebuliRPCConfiguration : NES::Configurations::BaseConfiguration
{
    /// GRPC Server Address URI. By default, it binds to any address and listens on port 8081
    Configurations::StringOption grpcAddressUri
        = {"listen",
           "[::]:8081",
           R"(The address to try to bind to the server in URI form. If
the scheme name is omitted, "dns:///" is assumed. To bind to any address,
please use IPv6 any, i.e., [::]:<port>, which also accepts IPv4
connections.  Valid values include dns:///localhost:1234,
192.168.1.1:31416, dns:///[::1]:27182, etc.)"};

    /// GRPC Server Address URI of the SingleNodeWorker.
    Configurations::StringOption targetAddressUri = {"target", "[::]:8080", R"(The Address of the SingleNode Worker)"};

protected:
    std::vector<BaseOption*> getOptions() override { return {&grpcAddressUri, &targetAddressUri}; }
};

CLI::Sink deserializeSink(const RegisterQueryRequest::Sink& request)
{
    CLI::Sink sink;
    for (auto entry : request.config())
    {
        sink.config.try_emplace(entry.first, entry.second);
    }
    sink.type = request.type();
    sink.name = request.name();
    return sink;
}
CLI::PhysicalSource deserializePhysicalSource(const RegisterQueryRequest::PhysicalSource& request)
{
    CLI::PhysicalSource physicalSource;
    physicalSource.logical = request.logical();
    for (auto entry : request.parserconfig())
    {
        physicalSource.parserConfig.try_emplace(entry.first, entry.second);
    }
    for (auto entry : request.sourceconfig())
    {
        physicalSource.sourceConfig.try_emplace(entry.first, entry.second);
    }
    return physicalSource;
}

CLI::LogicalSource deserializeLogicalSource(const RegisterQueryRequest::LogicalSource& request)
{
    CLI::LogicalSource logicalSource;
    logicalSource.name = request.name();
    for (auto& field : request.schema())
    {
        logicalSource.schema.emplace_back(field.name(), *magic_enum::enum_cast<BasicType>(field.type()));
    }
    return logicalSource;
}

CLI::QueryConfig deserializeQueryRequest(const RegisterQueryRequest* request)
{
    CLI::QueryConfig config;
    config.query = request->query();
    for (auto& logical : request->logical())
    {
        config.logical.emplace_back(deserializeLogicalSource(logical));
    }
    for (auto& physical : request->physical())
    {
        config.physical.emplace_back(deserializePhysicalSource(physical));
    }
    config.sinks[request->sink().name()] = deserializeSink(request->sink());
    return config;
}


struct GRPCService : NebuliRPCService::Service
{
    grpc::Status
    RegisterQuery(grpc::ServerContext*, const RegisterQueryRequest* request, RegisterQueryReply* response) override
    {
        auto config = deserializeQueryRequest(request);
        auto plan = CLI::createFullySpecifiedQueryPlan(config);
        auto id = client.registerQuery(*plan);
        response->set_queryid(id);
        return grpc::Status::OK;
    }

    grpc::Status
    UnregisterQuery(grpc::ServerContext* , const NES::UnregisterQueryRequest* request, google::protobuf::Empty* ) override
    {
       client.unregister(request->queryid());
       return grpc::Status::OK;
    }

    grpc::Status
    StartQuery(grpc::ServerContext* , const NES::StartQueryRequest* request, google::protobuf::Empty* ) override
    {
       client.start(request->queryid());
       return grpc::Status::OK;
    }

    grpc::Status StopQuery(grpc::ServerContext* , const NES::StopQueryRequest* request, google::protobuf::Empty* ) override
    {
        client.stop(request->queryid());
        return grpc::Status::OK;
    }

    explicit GRPCService(GRPCClient client) : client(std::move(client)) { }
    GRPCClient client;
};
}


int main(int argc, const char* argv[])
{
    try
    {
        NES::Logger::setupLogging("nebuli-rpc.log", NES::LogLevel::LOG_DEBUG);
        auto configuration = NES::Configurations::loadConfiguration<NES::NebuliRPCConfiguration>(argc, argv);
        if (!configuration)
        {
            return 0;
        }

        NES::GRPCClient client(CreateChannel(configuration->targetAddressUri, grpc::InsecureChannelCredentials()));
        NES::GRPCService nebuliRPC{std::move(client)};

        grpc::ServerBuilder builder;
        builder.AddListeningPort(configuration->grpcAddressUri, grpc::InsecureServerCredentials());
        builder.RegisterService(&nebuliRPC);

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