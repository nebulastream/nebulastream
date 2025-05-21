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
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/cpptrace.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/color.h>
#include <google/protobuf/text_format.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <NebuLI.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include "../private/Distributed/Topology.hpp"


#include "YAMLConfigLoader.hpp"

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

/// Helper function to convert milliseconds since epoch to a readable time string
std::string formatTimestamp(uint64_t timestampMs)
{
    auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(timestampMs));
    return fmt::format("{:%Y-%m-%d %H:%M:%S}.{:03d}", timePoint, timestampMs % 1000);
}

/// Function to pretty print the WorkerStatusResponse
void prettyPrintWorkerStatus(std::string_view grpcAddress, const WorkerStatusResponse& response)
{
    /// Print header
    fmt::print("=== Worker Status Report ===\n");
    fmt::print("Node: {}\n", grpcAddress);
    fmt::print("Report time: {}\n\n", formatTimestamp(response.afterunixtimestampinms()));

    /// Print active queries
    fmt::print("Active Queries: {}\n", response.activequeries_size());
    if (response.activequeries_size() > 0)
    {
        fmt::print("{:<12} {:<30}\n", "Query ID", "Started At");
        fmt::print("{:<12} {:<30}\n", "--------", "----------");

        for (const auto& query : response.activequeries())
        {
            fmt::print("{:<12} {:<30}\n", query.queryid(), formatTimestamp(query.startedunixtimestampinms()));
        }
        fmt::print("\n");
    }

    /// Print terminated queries
    fmt::print("Terminated Queries: {}\n", response.terminatedqueries_size());
    if (response.terminatedqueries_size() > 0)
    {
        fmt::print("{:<12} {:<30} {:<30} {:<30}\n", "Query ID", "Started At", "Terminated At", "Error");
        fmt::print("{:<12} {:<30} {:<30} {:<30}\n", "--------", "----------", "-------------", "-----");

        for (const auto& query : response.terminatedqueries())
        {
            std::string errorMsg = query.has_error() ? fmt::format("Error: {}", query.error().message()) : "None";

            fmt::print(
                "{:<12} {:<30} {:<30} {:<30}\n",
                query.queryid(),
                formatTimestamp(query.startedunixtimestampinms()),
                formatTimestamp(query.terminatedunixtimestampinms()),
                errorMsg);
        }
        fmt::print("\n");
    }

    fmt::print("=== End of Report ===\n");
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

    WorkerStatusResponse summary(std::chrono::system_clock::time_point after)
    {
        grpc::ClientContext context;
        WorkerStatusRequest request;
        request.set_afterunixtimestampinms(std::chrono::duration_cast<std::chrono::milliseconds>(after.time_since_epoch()).count());
        WorkerStatusResponse response;
        auto status = stub->RequestStatus(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Unregister was successful.");
        }
        else
        {
            throw NES::QueryStatusFailed(
                "Status: {}\nMessage: {}\nDetail: {}",
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details());
        }
        return response;
    }
};

/// To still support the legacy single node query configuration this function attempts to load either and returns a
/// Topology.
template <typename Parser>
std::pair<NES::Distributed::Config::Topology, std::optional<std::string>>
loadConfigurations(const argparse::ArgumentParser& arguments, const Parser& commandArguments)
{
    if (arguments.is_used("-t"))
    {
        if (commandArguments.is_used("-i"))
        {
            NES_ERROR("You cannot use the legacy query configuration (-i) in combination with a topology (-t)");
            exit(1);
        }

        auto path = std::filesystem::path(arguments.get<std::string>("-t"));
        try
        {
            if (commandArguments.is_used("query"))
            {
                return std::pair{
                    YAML::LoadFile(path).as<NES::Distributed::Config::Topology>(), commandArguments.template get<std::string>("query")};
            }
            return std::pair{YAML::LoadFile(path).as<NES::Distributed::Config::Topology>(), std::optional<std::string>{}};
        }
        catch (const std::exception& ex)
        {
            NES_ERROR("Could not load topology file from {}. {}", path, ex.what());
            exit(1);
        }
    }

    if (!commandArguments.is_used("-i"))
    {
        NES_ERROR("Neither a topology (-t) nor a legacy query configuration (-i) was supplied");
        exit(1);
    }
    auto host = commandArguments.is_used("-s") ? commandArguments.template get<std::string>("-s") : "no_host_specified";
    auto path = std::filesystem::path(commandArguments.template get<std::string>("-i"));
    try
    {
        auto queryConfig = YAML::LoadFile(path).as<NES::CLI::QueryConfig>();
        return {NES::Distributed::Config::Topology::from(queryConfig, host), queryConfig.query};
    }
    catch (const std::exception& ex)
    {
        NES_ERROR("Could not load legacy query configuration file from {}. {}", path, ex.what());
        exit(1);
    }
}

void doRegister(argparse::ArgumentParser& parser)
{
    auto& registerArgs = parser.at<argparse::ArgumentParser>("register");
    auto [topology, query] = loadConfigurations(parser, registerArgs);
    if (!query)
    {
        NES_ERROR("No query was provided");
        exit(1);
    }

    auto plans = NES::CLI::createFullySpecifiedQueryPlan(*query, topology);
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
void doDump(argparse::ArgumentParser& parser)
{
    auto& dumpArgs = parser.at<argparse::ArgumentParser>("dump");
    auto [topology, query] = loadConfigurations(parser, dumpArgs);
    if (!query)
    {
        NES_ERROR("No query was provided");
        exit(1);
    }
    auto plans = NES::CLI::createFullySpecifiedQueryPlan(*query, topology);
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

            outputPath = std::filesystem::path(outputPath) / fmt::format("{}.bin", decomposedQueryPlan->getGRPC());
            file = std::ofstream(outputPath);
            if (!file)
            {
                NES_FATAL_ERROR("Could not open output file: {}", outputPath);
                std::exit(1);
            }
            outputStream = &file;
        }

        if (!serialized.SerializeToOstream(outputStream))
        {
            NES_FATAL_ERROR("Failed to write message to file.");
            exit(1);
        }

        if (outputPath != "-")
        {
            NES_INFO("Wrote protobuf to {}", outputPath);
        }
    }
}


void doStatus(argparse::ArgumentParser& parser)
{
    auto& statusArgs = parser.at<argparse::ArgumentParser>("status");
    auto [topology, _] = loadConfigurations(parser, statusArgs);

    std::chrono::system_clock::time_point after(std::chrono::milliseconds(0));
    if (statusArgs.is_used("--after"))
    {
        after = std::chrono::system_clock::time_point(std::chrono::milliseconds(statusArgs.get<size_t>("--after")));
    }

    for (const auto& node : topology.nodes)
    {
        GRPCClient client(grpc::CreateChannel(node.grpc, grpc::InsecureChannelCredentials()));
        try
        {
            auto summary = client.summary(after);
            prettyPrintWorkerStatus(node.grpc, summary);
        }
        catch (...)
        {
            NES::tryLogCurrentException();
            NES_ERROR("Failed to get summary of node at {}", node.grpc);
        }
        fmt::print("\n");
    }
}

int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_ERROR);
        using argparse::ArgumentParser;
        ArgumentParser program("nebuli");
        program.add_argument("-d", "--debug").flag().help("Dump the Query plan and enable debug logging");
        program.add_argument("-t", "--topology").nargs(1).help("Enables distributed deployment, based on a topology");

        ArgumentParser registerQuery("register");
        registerQuery.add_argument("query").help("Query Statement").default_value("");
        registerQuery.add_argument("-i").nargs(1).help("Legacy SingleNodeQuery configuration");
        registerQuery.add_argument("-s").nargs(1).help("Legacy SingleNode server address");
        registerQuery.add_argument("-x").flag();

        ArgumentParser startQuery("start");
        startQuery.add_argument("queryId");

        ArgumentParser stopQuery("stop");
        stopQuery.add_argument("queryId");

        ArgumentParser unregisterQuery("unregister");
        unregisterQuery.add_argument("queryId");

        ArgumentParser dump("dump");
        dump.add_argument("-o", "--output").default_value("-").help("Write the DecomposedQueryPlan to file. Use - for stdout");
        dump.add_argument("-i").nargs(1).help("Legacy SingleNode query configuration");
        dump.add_argument("-s").nargs(1).help("Legacy SingleNode server address");
        dump.add_argument("query").help("Query Statement").default_value("");

        ArgumentParser status("status");
        status.add_argument("-i").nargs(1).help("Legacy SingleNodeQuery configuration");
        status.add_argument("-s").nargs(1).help("Legacy SingleNode server address");
        status.add_argument("--after").nargs(1).default_value("Request only status updates that happens after this unix timestamp.");

        program.add_subparser(registerQuery);
        program.add_subparser(startQuery);
        program.add_subparser(stopQuery);
        program.add_subparser(unregisterQuery);
        program.add_subparser(dump);
        program.add_subparser(status);

        program.parse_args(argc, argv);

        if (program.get<bool>("-d"))
        {
            NES::Logger::getInstance()->changeLogLevel(NES::LogLevel::LOG_DEBUG);
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

        if (program.is_subcommand_used("status"))
        {
            doStatus(program);
            return 0;
        }


        if (program.is_subcommand_used("dump"))
        {
            doDump(program);
            return 0;
        }

        if (program.is_subcommand_used("register"))
        {
            doRegister(program);
            return 0;
        }
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentExceptionCode();
    }

    NES_ERROR("Unhandled command")
    return 1;
}
