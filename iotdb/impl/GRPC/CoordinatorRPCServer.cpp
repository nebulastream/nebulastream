#include <GRPC/CoordinatorRPCServer.hpp>

CoordinatorRPCServer::CoordinatorRPCServer() {
    std::string server_address("0.0.0.0:50051");
    CoordinatorRPCServerImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}
