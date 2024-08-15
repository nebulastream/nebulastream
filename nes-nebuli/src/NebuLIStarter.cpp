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

#include <fstream>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <argparse/argparse.hpp>
#include <google/protobuf/text_format.h>
#include <grpc++/create_channel.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>

#include <NebuLI.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>

namespace NES
{
class TCPSourceType;
}
using namespace std::literals;


class GRPCClient
{
public:
    explicit GRPCClient(std::shared_ptr<grpc::Channel> channel) : stub(WorkerRPCService::NewStub(channel)) { }
    std::unique_ptr<WorkerRPCService::Stub> stub;

    size_t registerQuery(NES::DecomposedQueryPlanPtr plan)
    {
        grpc::ClientContext context;
        RegisterQueryReply reply;
        RegisterQueryRequest request;
        NES::DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(plan, request.mutable_decomposedqueryplan());
        auto status = stub->RegisterQuery(&context, request, &reply);
        if (status.ok())
        {
            NES_DEBUG("Registration was successful.");
        }
        else
        {
            NES_THROW_RUNTIME_ERROR(fmt::format(
                "Registration failed. Status: {}\nMessage: {}\nDetail: {}",
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details()));
        }
        return reply.queryid();
    }

    void stop(size_t queryId)
    {
        grpc::ClientContext context;
        StopQueryRequest request;
        request.set_queryid(queryId);
        request.set_terminationtype(StopQueryRequest_QueryTerminationType_HardStop);
        google::protobuf::Empty response;
        auto status = stub->StopQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Stopping was successful.");
        }
        else
        {
            NES_THROW_RUNTIME_ERROR(fmt::format(
                "Registration failed. Status: {}\nMessage: {}\nDetail: {}",
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details()));
        }
    }

    void start(size_t queryId)
    {
        grpc::ClientContext context;
        StartQueryRequest request;
        google::protobuf::Empty response;
        request.set_queryid(queryId);
        auto status = stub->StartQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Starting was successful.");
        }
        else
        {
            NES_THROW_RUNTIME_ERROR(fmt::format(
                "Registration failed. Status: {}\nMessage: {}\nDetail: {}",
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details()));
        }
    }

    void unregister(size_t queryId)
    {
        grpc::ClientContext context;
        UnregisterQueryRequest request;
        google::protobuf::Empty response;
        request.set_queryid(queryId);
        auto status = stub->UnregisterQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Unregister was successful.");
        }
        else
        {
            NES_THROW_RUNTIME_ERROR(fmt::format(
                "Registration failed. Status: {}\nMessage: {}\nDetail: {}",
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details()));
        }
    }
};


int main(int argc, char** argv)
{
    using namespace NES;
    using namespace argparse;
    ArgumentParser program("nebuli");
    program.add_argument("-d", "--debug").flag().help("Dump the Query plan and enable debug logging");

    ArgumentParser registerQuery("register");
    registerQuery.add_argument("-s", "--server").help("grpc uri e.g., 127.0.0.1:8080");
    registerQuery.add_argument("-i", "--input").default_value("-").help("Read the query description. Use - for stdin which is the default");
    registerQuery.add_argument("-x").flag();

    ArgumentParser startQuery("start");
    startQuery.add_argument("queryId").scan<'i', size_t>();
    startQuery.add_argument("-s", "--server").help("grpc uri e.g., 127.0.0.1:8080");

    ArgumentParser stopQuery("stop");
    stopQuery.add_argument("queryId").scan<'i', size_t>();
    stopQuery.add_argument("-s", "--server").help("grpc uri e.g., 127.0.0.1:8080");

    ArgumentParser unregisterQuery("unregister");
    unregisterQuery.add_argument("queryId").scan<'i', size_t>();
    unregisterQuery.add_argument("-s", "--server").help("grpc uri e.g., 127.0.0.1:8080");

    ArgumentParser dump("dump");
    dump.add_argument("-o", "--output").default_value("-").help("Write the DecomposedQueryPlan to file. Use - for stdout");
    dump.add_argument("-i", "--input").default_value("-").help("Read the query description. Use - for stdin which is the default");

    program.add_subparser(registerQuery);
    program.add_subparser(startQuery);
    program.add_subparser(stopQuery);
    program.add_subparser(unregisterQuery);
    program.add_subparser(dump);

    try
    {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& err)
    {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        std::exit(1);
    }

    if (program.get<bool>("-d"))
    {
        NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_DEBUG);
    }
    else
    {
        NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_ERROR);
    }

    auto handled = [&](const std::initializer_list<std::pair<std::string_view, void (GRPCClient::*)(size_t)>>& pairs)
    {
        for (const auto& [name, fn] : pairs)
        {
            if (program.is_subcommand_used(name))
            {
                auto& parser = program.at<ArgumentParser>(name);
                auto serverUri = parser.get<std::string>("-s");
                GRPCClient client(grpc::CreateChannel(serverUri, grpc::InsecureChannelCredentials()));
                auto queryId = parser.get<size_t>("queryId");
                (client.*fn)(queryId);
                return true;
            }
        }
        return false;
    }({{"start", &GRPCClient::start}, {"unregister", &GRPCClient::unregister}, {"stop", &GRPCClient::stop}});
    if (handled)
    {
        return 0;
    }

    DecomposedQueryPlanPtr decomposedQueryPlan;
    try
    {
        std::string command = program.is_subcommand_used("register") ? "register" : "dump";
        auto input = program.at<ArgumentParser>(command).get("-i");
        if (input == "-")
        {
            decomposedQueryPlan = NES::CLI::loadFrom(std::cin);
        }
        else
        {
            decomposedQueryPlan = NES::CLI::loadFromFile(input);
        }
    }
    catch (...)
    {
        tryLogCurrentException();
        exit(1);
    }

    std::string output;
    SerializableDecomposedQueryPlan serialized;
    DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(decomposedQueryPlan, &serialized);
    google::protobuf::TextFormat::PrintToString(serialized, &output);
    NES_INFO("GRPC QueryPlan: {}", output);
    if (program.is_subcommand_used("dump"))
    {
        auto& dumpArgs = program.at<ArgumentParser>("dump");
        auto intput = dumpArgs.get<std::string>("-o");
        std::ostream* output;
        std::ofstream file;
        if (intput == "-")
        {
            output = &std::cout;
        }
        else
        {
            file = std::ofstream(intput);
            if (!file)
            {
                NES_FATAL_ERROR("Could not open output file: {}", intput);
                std::exit(1);
            }
            output = &file;
        }
        SerializableDecomposedQueryPlan serialized;
        DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(decomposedQueryPlan, &serialized);
        if (!serialized.SerializeToOstream(output))
        {
            NES_FATAL_ERROR("Failed to write message to file.");
            return -1;
        }

        if (intput == "-")
        {
            NES_INFO("Wrote protobuf to {}", intput);
        }
    }
    else if (program.is_subcommand_used("register"))
    {
        auto& registerArgs = program.at<ArgumentParser>("register");
        GRPCClient client(grpc::CreateChannel(registerArgs.get<std::string>("-s"), grpc::InsecureChannelCredentials()));
        auto queryId = client.registerQuery(decomposedQueryPlan);
        if (registerArgs.is_used("-x"))
        {
            client.start(queryId);
        }
        std::cout << queryId;
    }

    return 0;
}
