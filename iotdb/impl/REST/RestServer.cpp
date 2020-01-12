#include <iostream>
#include <REST/usr_interrupt_handler.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/Controller/RestController.hpp>
#include <REST/RestServer.hpp>
#include <Util/Logger.hpp>
using namespace NES;

bool RestServer::start(std::string host,
                       u_int16_t port,
                       infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle) {

    std::cout << "IoT-DB REST server port set to " << port << std::endl;
    std::cout << "To adjust the value supply: --rest_host <value> --rest_port <value>, as command line argument"
              << std::endl;
    std::cout << "------------------------------------------------------------" << std::endl;

    RestController server;
    server.setCoordinatorActorHandle(coordinatorActorHandle);
    server.setEndpoint("http://" + host + ":" + std::to_string(port) + "/v1/iotdb/");

    try {
        // wait for server initialization...
        server.accept().wait();
        std::cout << "REST Server started\n";
        std::cout << "REST Server now listening for requests at: " << server.endpoint() << '\n';
        InterruptHandler::waitForUserInterrupt();
        server.shutdown().wait();
    } catch (std::exception& e) {
        IOTDB_ERROR("Unable to start REST server");
        return false;
    } catch (...) {
        RuntimeUtils::printStackTrace();
        return false;
    }
    return true;
}





