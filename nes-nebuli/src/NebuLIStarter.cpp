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

#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <argparse/argparse.hpp>
#include <fmt/color.h>
#include <google/protobuf/text_format.h>
#include <grpc++/create_channel.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <NebuLI.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include "YAMLConfigLoader.hpp" /// NOLINT(misc-include-cleaner) used indirectly by yaml-cpp

using namespace std::literals;


struct DistributedQueryId
{
    struct ConnectionQueryIdPair
    {
        std::string connection;
        size_t id;
    };
    std::vector<ConnectionQueryIdPair> queries;
    static DistributedQueryId load(std::string identifier)
    {
        return YAML::LoadFile(std::filesystem::path("/tmp") / identifier).as<DistributedQueryId>();
    }
    static std::string save(const DistributedQueryId& queryId);
};

namespace YAML
{
template <>
struct convert<DistributedQueryId::ConnectionQueryIdPair>
{
    static bool decode(const Node& node, DistributedQueryId::ConnectionQueryIdPair& rhs)
    {
        rhs.connection = node["connection"].as<std::string>();
        rhs.id = node["id"].as<size_t>();
        return true;
    }
    static Node encode(const DistributedQueryId::ConnectionQueryIdPair& rhs)
    {
        Node node;
        node["id"] = rhs.id;
        node["connection"] = rhs.connection;
        return node;
    }
};

template <>
struct convert<DistributedQueryId>
{
    static bool decode(const Node& node, DistributedQueryId& rhs)
    {
        rhs.queries = node["queries"].as<std::vector<DistributedQueryId::ConnectionQueryIdPair>>();
        return true;
    }
    static Node encode(const DistributedQueryId& rhs)
    {
        Node node;
        node["queries"] = rhs.queries;
        return node;
    }
};
}


std::string DistributedQueryId::save(const DistributedQueryId& queryId)
{
    std::string name1 = std::tmpnam(nullptr);
    std::ofstream out(name1);
    if (out.is_open())
    {
        YAML::Emitter emitter{out};
        YAML::Node node{queryId};
        out << node;
    }

    auto withoutPrefix = std::string_view(name1);
    withoutPrefix.remove_prefix(5);
    return std::string(withoutPrefix);
}

class GRPCClient
{
public:
    explicit GRPCClient(std::shared_ptr<grpc::Channel> channel) : stub(WorkerRPCService::NewStub(channel)) { }
    std::unique_ptr<WorkerRPCService::Stub> stub;

    size_t registerQuery(const NES::DecomposedQueryPlan& plan) const
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
            throw NES::QueryRegistrationFailed(
                "Status: {}\nMessage: {}\nDetail: {}",
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details());
        }
        return reply.queryid();
    }

    void stop(size_t queryId) const
    {
        grpc::ClientContext context;
        StopQueryRequest request;
        request.set_queryid(queryId);
        request.set_terminationtype(StopQueryRequest::HardStop);
        google::protobuf::Empty response;
        auto status = stub->StopQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Stopping was successful.");
        }
        else
        {
            throw NES::QueryStopFailed(
                "Status: {}\nMessage: {}\nDetail: {}",
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details());
        }
    }

    void status(size_t queryId) const
    {
        grpc::ClientContext context;
        QuerySummaryRequest request;
        request.set_queryid(queryId);
        QuerySummaryReply response;
        auto status = stub->RequestQuerySummary(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Status was successful.");
        }
        else
        {
            throw NES::QueryStatusFailed(
                "Status: {}\nMessage: {}\nDetail: {}",
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details());
        }
    }

    void start(size_t queryId) const
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
            throw NES::QueryStartFailed(
                "Status: {}\nMessage: {}\nDetail: {}",
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details());
        }
    }

    void unregister(size_t queryId) const
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
            throw NES::QueryUnregistrationFailed(
                "Status: {}\nMessage: {}\nDetail: {}",
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details());
        }
    }
};

std::pair<NES::CLI::QueryConfig, NES::Distributed::Config::Topology> loadConfigurations(argparse::ArgumentParser& arguments)
{
    std::optional<NES::Distributed::Config::Topology> topology;
    if (arguments.is_used("-t"))
    {
        auto path = std::filesystem::path(arguments.get<std::string>("-t"));
        try
        {
            topology = YAML::LoadFile(path).as<NES::Distributed::Config::Topology>();
        }
        catch (const std::exception& ex)
        {
            NES_ERROR("Could not load topology file from {}. {}", path, ex.what());
            exit(1);
        }
    }

    NES::CLI::QueryConfig config;
    if (arguments.is_subcommand_used("dump") || arguments.is_subcommand_used("register"))
    {
        const std::string command = arguments.is_subcommand_used("register") ? "register" : "dump";
        auto input = arguments.at<argparse::ArgumentParser>(command).get("-i");

        if (input == "-")
        {
            try
            {
                config = YAML::Load(std::cin).as<NES::CLI::QueryConfig>();
            }
            catch (const std::exception& ex)
            {
                NES_ERROR("Could not load query configuration file from stdin. {}", ex.what());
                exit(1);
            }
        }
        else
        {
            try
            {
                config = YAML::LoadFile(input).as<NES::CLI::QueryConfig>();
            }
            catch (const std::exception& ex)
            {
                NES_ERROR("Could not load query configuration file from {}. {}", input, ex.what());
                exit(1);
            }
        }
    }

    if (!topology)
    {
        if (!arguments.is_used("-s"))
        {
            NES_ERROR("Either a topology file or the `-s` parameter is required.");
            exit(1);
        }
        auto server = arguments.get<std::string>("-s");
        topology = NES::Distributed::Config::Topology::from(config, arguments.get("-s"));
    }

    return {config, *topology};
}

void doRegister(argparse::ArgumentParser& parser, std::vector<std::shared_ptr<NES::DecomposedQueryPlan>> plans)
{
    auto& registerArgs = parser.at<argparse::ArgumentParser>("register");

    DistributedQueryId distributedQueryId;
    for (const auto& plan : plans)
    {
        GRPCClient client(grpc::CreateChannel(plan->getGRPC(), grpc::InsecureChannelCredentials()));
        auto queryId = client.registerQuery(*plan);
        distributedQueryId.queries.emplace_back(plan->getGRPC(), queryId);
    }

    if (registerArgs.is_used("-x"))
    {
        for (auto& [grpc, queryId] : distributedQueryId.queries)
        {
            GRPCClient client(grpc::CreateChannel(grpc, grpc::InsecureChannelCredentials()));
            client.start(queryId);
        }
    }

    std::cout << DistributedQueryId::save(distributedQueryId) << '\n';
}
void doDump(argparse::ArgumentParser& parser, std::vector<std::shared_ptr<NES::DecomposedQueryPlan>> plans)
{
    auto& dumpArgs = parser.at<argparse::ArgumentParser>("dump");
    for (const auto& decomposedQueryPlan : plans)
    {
        auto outputPath = dumpArgs.get<std::string>("-o");
        NES::SerializableDecomposedQueryPlan serialized;
        NES::DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(*decomposedQueryPlan, &serialized);
        std::ostream* outputStream;
        std::ofstream file;

        if (outputPath == "-")
        {
            outputStream = &std::cout;
        }
        else
        {
            fmt::print(fmt::emphasis::bold | fg(fmt::color::light_cyan), "{}\n", std::string(80, '='));
            fmt::print(fmt::emphasis::bold | fg(fmt::color::light_cyan), "Node at {}\n", decomposedQueryPlan->getGRPC());
            fmt::print(fmt::emphasis::bold | fg(fmt::color::light_cyan), "{}\n", std::string(80, '-'));
            fmt::print(fmt::emphasis::bold | fg(fmt::color::light_cyan), "{}\n", decomposedQueryPlan->toString());
            fmt::print(fmt::emphasis::bold | fg(fmt::color::light_cyan), "{}\n", std::string(80, '='));

            if (!std::filesystem::is_directory(outputPath))
            {
                NES_FATAL_ERROR("Output should be a directory.");
                exit(1);
            }

            outputPath = std::filesystem::path(outputPath) / fmt::format("{}.txtpb", decomposedQueryPlan->getGRPC());
            file = std::ofstream(outputPath);
            if (!file)
            {
                NES_FATAL_ERROR("Could not open output file: {}", outputPath);
                std::exit(1);
            }
            outputStream = &file;
        }

        *outputStream << serialized.DebugString() << std::endl;

        if (outputPath != "-")
        {
            NES_INFO("Wrote protobuf to {}", outputPath);
        }
    }
}

int main(int argc, char** argv)
{
    using namespace NES;
    using namespace argparse;
    ArgumentParser program("nebuli");
    program.add_argument("-d", "--debug").flag().help("Dump the Query plan and enable debug logging");
    program.add_argument("-s", "--server").help("grpc uri e.g., 127.0.0.1:8080");
    program.add_argument("-t", "--topology").nargs(1).help("Enables distributed deployment, based on a topology");

    ArgumentParser registerQuery("register");
    registerQuery.add_argument("-i", "--input").default_value("-").help("Read the query description. Use - for stdin which is the default");
    registerQuery.add_argument("-x").flag();

    ArgumentParser startQuery("start");
    startQuery.add_argument("queryId");

    ArgumentParser stopQuery("stop");
    stopQuery.add_argument("queryId");

    ArgumentParser unregisterQuery("unregister");
    unregisterQuery.add_argument("queryId");

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


    bool handled = false;
    for (const auto& [name, fn] : std::initializer_list<std::pair<std::string_view, void (GRPCClient::*)(size_t) const>>{
             {"start", &GRPCClient::start}, {"unregister", &GRPCClient::unregister}, {"stop", &GRPCClient::stop}})
    {
        if (program.is_subcommand_used(name))
        {
            auto& parser = program.at<ArgumentParser>(name);
            auto distributedQueryId = DistributedQueryId::load(parser.get<std::string>("queryId"));
            for (auto& [connection, queryId] : distributedQueryId.queries)
            {
                GRPCClient client(CreateChannel(connection, grpc::InsecureChannelCredentials()));
                (client.*fn)(queryId);
            }
            handled = true;
            break;
        }
    }

    if (handled)
    {
        return 0;
    }

    auto [config, topology] = loadConfigurations(program);

    std::vector<std::shared_ptr<DecomposedQueryPlan>> plans;
    try
    {
        const std::string command = program.is_subcommand_used("register") ? "register" : "dump";
        auto input = program.at<ArgumentParser>(command).get("-i");
        plans = CLI::createFullySpecifiedQueryPlan(config.query, topology);
    }
    catch (...)
    {
        tryLogCurrentException();
        exit(1);
    }

    if (program.is_subcommand_used("dump"))
    {
        doDump(program, plans);
        return 0;
    }

    if (program.is_subcommand_used("register"))
    {
        doRegister(program, plans);
        return 0;
    }


    NES_ERROR("Unhandled command")
    return 1;
}
