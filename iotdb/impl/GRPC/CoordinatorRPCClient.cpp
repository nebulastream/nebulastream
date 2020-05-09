#include <GRPC/CoordinatorRPCClient.hpp>
#include <GRPC/CoordinatorRPCClientImpl.hpp>

CoordinatorRPCClient::CoordinatorRPCClient()
{
    CoordinatorRPCClientImpl client(grpc::CreateChannel("",
        grpc::InsecureChannelCredentials()));
    std::string user("world");
//    std::string reply = greeter.SayHello(user);
//    std::cout << "Greeter received: " << reply << std::endl;
}
