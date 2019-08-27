#include <iostream>
#include <REST/usr_interrupt_handler.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/Controller/RestController.hpp>
#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>

using namespace web;
using namespace iotdb;
using namespace std;

namespace po = boost::program_options;
const uint16_t REST_PORT = 8081;

int main(int argc, const char *argv[]) {
    InterruptHandler::hookSIGINT();



    /* Important Variables with defaults. */
    std::string host = "localhost";
    uint16_t port = REST_PORT;

    /* Define program options: connection to server. */
    po::options_description connection_options("Connection Parameters");
    connection_options.add_options()("rest_host", po::value<std::string>(),
                                     "Set IoT-DB REST server host (default: localhost).")(
            "rest_port", po::value<uint16_t>(), "Set IoT-DB REST server port (default: 8081).");

    /* All program options for command line. */
    po::options_description commandline_options;
    commandline_options.add(connection_options);

    /* Parse parameters. */
    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, commandline_options), vm);
        po::notify(vm);
    }
    catch (const std::exception &e) {
        std::cerr << "Failure while parsing connection parameters!" << std::endl;
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    if (vm.count("rest_host")) {
        host = vm["rest_host"].as<std::string>();
    }
    std::cout << "IoT-DB REST server host set to " << host << std::endl;

    if (vm.count("rest_port")) {
        port = vm["rest_port"].as<uint16_t>();
    }
    std::cout << "IoT-DB REST server port set to " << port << std::endl;

    std::cout << "To adjust the value supply: --rest_host <value> --rest_port <value>, as command line argument" << std::endl;
    std::cout << "------------------------------------------------------------" << std::endl;

    RestController server;
    server.setEndpoint("http://" + host + ":" + to_string(port) + "/v1/iotdb/");

    try {
        // wait for server initialization...
        server.accept().wait();
        std::cout << "Modern C++ Microservice now listening for requests at: " << server.endpoint() << '\n';
        std::cout << "REST Server started" << std::endl;
        InterruptHandler::waitForUserInterrupt();

        server.shutdown().wait();
    }
    catch (std::exception &e) {
        std::cerr << "something wrong happen! :(" << '\n';
    }
    catch (...) {
        RuntimeUtils::printStackTrace();
    }

    return 0;
}

