/********************************************************
 *     _   _   ______    _____
 *    | \ | | |  ____|  / ____|
 *    |  \| | | |__    | (___
 *    | . ` | |  __|    \___ \     Coordinator
 *    | |\  | | |____   ____) |
 *    |_| \_| |______| |_____/
 *
 ********************************************************/
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>
#include <Components/NesCoordinator.hpp>
#include <caf/io/all.hpp>
#include <Actors/AtomUtils.hpp>
#include <Util/Logger.hpp>

namespace po = boost::program_options;
using namespace NES;

const string logo = "/********************************************************\n"
                    " *     _   _   ______    _____\n"
                    " *    | \\ | | |  ____|  / ____|\n"
                    " *    |  \\| | | |__    | (___\n"
                    " *    | . ` | |  __|    \\___ \\     Coordinator\n"
                    " *    | |\\  | | |____   ____) |\n"
                    " *    |_| \\_| |______| |_____/\n"
                    " *\n"
                    " ********************************************************/";


/*** DEBUG CODE ***/

#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <GRPC/helloworld.grpc.pb.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using helloworld::HelloRequest;
using helloworld::HelloReply;
using helloworld::Greeter;

// Logic and data behind the server's behavior.
class GreeterServiceImpl final : public Greeter::Service {
    Status SayHello(ServerContext* context, const HelloRequest* request,
                    HelloReply* reply) override {
        std::string prefix("Hello ");
        reply->set_message(prefix + request->name());
        return Status::OK;
    }
};
void RunServer() {
    std::string server_address("0.0.0.0:50051");
    GreeterServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
//
//    grpc::EnableDefaultHealthCheckService(true);
//    //    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
//    ServerBuilder builder;
//    // Listen on the given address without any authentication mechanism.
//    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
//    // Register "service" as the instance through which we'll communicate with
//
//    // clients. In this case it corresponds to an *synchronous* service.
//    builder.RegisterService(&service);
//    // Finally assemble the server.
//    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}




int main(int argc, const char* argv[]) {
    NES::setupLogging("nesCoordinatorStarter.log", NES::LOG_DEBUG);
    cout << logo << endl;

    RunServer();

    // Initializing defaults
    uint16_t port = 8081;
    uint16_t actorPort = 0;
    std::string host = "localhost";
    std::string serverIp = "localhost";

    po::options_description serverOptions("Nes Coordinator Server Options");
    serverOptions.add_options()(
        "rest_host", po::value<std::string>(),
        "Set NES Coordinator server host address (default: localhost).")
        ("server_ip", po::value<string>(&serverIp)->default_value(serverIp),
         "Set NES server ip (default: localhost).")
        (
            "rest_port", po::value<uint16_t>(),
            "Set NES REST server port (default: 8081).")(
        "actor_port", po::value<uint16_t>(&actorPort)->default_value(actorPort),
        "Set NES actor server port (default: 0).")
        ("help", "Display help message");


    /* Parse parameters. */
    po::variables_map vm;
    try {
        po::store(po::command_line_parser(argc, argv).options(serverOptions).run(), vm);
        po::notify(vm);
    } catch (const std::exception& e) {
        std::cerr << "Failure while parsing connection parameters!" << std::endl;
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    if (vm.count("help")) {
        std::cout << "Basic Command Line Parameter " << std::endl << serverOptions
                  << std::endl;
        return 0;
    }

    bool changed = false;
    if (vm.count("rest_host")) {
        host = vm["rest_host"].as<std::string>();
        changed = true;
    }

    if (vm.count("rest_port")) {
        port = vm["rest_port"].as<uint16_t>();
        changed = true;
    }

    cout << "creating coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();

    if (changed) {
        cout << "config changed thus rest params" << endl;
        crd->setRestConfiguration(host, port);
    }
    if (serverIp != "localhost") {
        cout << "set server ip to " << serverIp << endl;
        crd->setServerIp(serverIp);
    }

    cout << "start coordinator with port " << actorPort << endl;
    crd->startCoordinator(/**blocking**/true, actorPort);

    cout << "coordinator started" << endl;
}

