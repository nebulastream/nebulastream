#include <iostream>
#include <Rest/usr_interrupt_handler.hpp>
#include <Rest/runtime_utils.hpp>
#include <Rest/svc_controller.hpp>


using namespace web;
using namespace cfx;
using namespace std;

const uint16_t REST_PORT = 8081;
const string HOST_NAME = "127.0.0.1";

int main(int argc, const char *argv[]) {
    InterruptHandler::hookSIGINT();

    ServiceController server;
    server.setEndpoint("http://"+HOST_NAME+":"+ to_string(REST_PORT) +"/v1/iotdb/");

    try {
        // wait for server initialization...
        server.accept().wait();
        std::cout << "Modern C++ Microservice now listening for requests at: " << server.endpoint() << '\n';

        InterruptHandler::waitForUserInterrupt();

        server.shutdown().wait();
    }
    catch(std::exception & e) {
        std::cerr << "somehitng wrong happen! :(" << '\n';
    }
    catch(...) {
        RuntimeUtils::printStackTrace();
    }

    return 0;
}
