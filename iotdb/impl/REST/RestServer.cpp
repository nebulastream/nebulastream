#include <iostream>
#include <REST/usr_interrupt_handler.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/Controller/RestController.hpp>
#include <REST/RestServer.hpp>

using namespace iotdb;

bool RestServer::start(std::string host, u_int16_t port) {

  std::cout << "IoT-DB REST server port set to " << port << std::endl;
  std::cout << "To adjust the value supply: --host <value> --rest_port <value>, as command line argument"
            << std::endl;
  std::cout << "------------------------------------------------------------" << std::endl;

  RestController server;
  server.setEndpoint("http://" + host + ":" + to_string(port) + "/v1/iotdb/");

  try {
    // wait for server initialization...
    server.accept().wait();
    std::cout << "REST Server started\n";
    std::cout << "REST Server now listening for requests at: " << server.endpoint() << '\n';
    InterruptHandler::waitForUserInterrupt();
    server.shutdown().wait();
  } catch (std::exception &e) {
    std::cerr << "something wrong happen! :(" << '\n';
    return false;
  } catch (...) {
    RuntimeUtils::printStackTrace();
    return false;
  }
  return true;
}





