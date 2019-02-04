/**
 *
 * In this file the client side of the IoT-DB is implemented.
 * With this program the user is able to control the DBMS.
 *
 * Tasks:
 *   - Connect to a server on a specified port (parameters of executable)
 *   - Send a query (*.cpp) to the server, get query id back
 *   - Request a list of current queries on the server
 *   - Remove a query from the server
 *
 */

#include <cstddef>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <zmq.hpp>

#ifndef MASTER_PORT
#define MASTER_PORT 38938
#endif // MASTER_PORT

namespace IotClient {

namespace fs = boost::filesystem;
namespace po = boost::program_options;

enum CLIENT_COMMAND { ADD_QUERY, REMOVE_QUERY, LIST_QUERIES };

/**
 * Check, if two program options are conflicting
 * (source: https://stackoverflow.com/a/29016720).
 */
void conflicting_options(const po::variables_map &vm, const char *opt1, const char *opt2) {
  if (vm.count(opt1) && !vm[opt1].defaulted() && vm.count(opt2) && !vm[opt2].defaulted())
    throw std::logic_error(std::string("Conflicting options '") + opt1 + "' and '" + opt2 + "'.");
}

/**
 * Sends *.cpp source file with query to Server via ZeroMQ.
 * Returns the id of the query on server.
 */
bool add_query(std::string file_path, std::string host, uint16_t port) {

  /* Check if file can be found on system and read. */
  fs::path path{file_path.c_str()};
  if (!fs::exists(path) || !fs::is_regular_file(path)) {
    std::cerr << "No file found at given path." << std::endl;
    return EXIT_FAILURE;
  }
  std::string file_name = path.filename().string();
  std::ifstream ifs(path.string().c_str());
  std::string file_content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

  /* Connect to server. */
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_REQ);
  std::string addr = "tcp://" + host + ":" + std::to_string(port);
  socket.connect(addr.c_str());

  /* Pack message and send to server. */
  CLIENT_COMMAND command = ADD_QUERY;
  size_t file_name_size = file_name.size();
  size_t file_content_size = file_content.size();

  zmq::message_t request(sizeof(CLIENT_COMMAND) + 2 * sizeof(size_t) + file_name.size() + file_content.size());
  std::memcpy(request.data(), &command, sizeof(CLIENT_COMMAND));
  std::memcpy((char *)request.data() + sizeof(CLIENT_COMMAND), &file_name_size, sizeof(size_t));
  std::memcpy((char *)request.data() + sizeof(CLIENT_COMMAND) + sizeof(size_t), &file_content_size, sizeof(size_t));
  std::memcpy((char *)request.data() + sizeof(CLIENT_COMMAND) + 2 * sizeof(size_t), file_name.data(), file_name_size);
  std::memcpy((char *)request.data() + sizeof(CLIENT_COMMAND) + 2 * sizeof(size_t) + file_name_size,
              file_content.data(), file_content_size);

  socket.send(request);

  /* Receive query id from server. */
  size_t query_id = 0;
  zmq::message_t reply;
  socket.recv(&reply);
  std::memcpy(&query_id, reply.data(), sizeof(size_t));

  /* Print info for user. */
  std::cout << "Query successfully send to IoT-DB with query id " << query_id << "!" << std::endl;

  socket.close();
  context.close();

  return EXIT_SUCCESS;
}

/**
 * Removes a query with given query id from the server.
 */
bool remove_query(size_t query_id, std::string host, uint16_t port) {

  /* Connect to server. */
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_REQ);
  std::string addr = "tcp://" + host + ":" + std::to_string(port);
  socket.connect(addr.c_str());

  /* Pack message and send to server. */
  CLIENT_COMMAND command = REMOVE_QUERY;
  zmq::message_t request(sizeof(CLIENT_COMMAND) + sizeof(size_t));
  std::memcpy(request.data(), &command, sizeof(CLIENT_COMMAND));
  std::memcpy((char *)request.data() + sizeof(CLIENT_COMMAND), &query_id, sizeof(size_t));
  socket.send(request);

  /* Receive query id from server. */
  bool removed = false;
  zmq::message_t reply;
  socket.recv(&reply);
  std::memcpy(&removed, reply.data(), sizeof(bool));

  /* Print info for user. */
  if (removed) {
    std::cout << "Query with id " << query_id << " successfully removed from IoT-DB!" << std::endl;
  } else {
    std::cout << "Query with id " << query_id << " not found on IoT-DB!" << std::endl;
  }

  socket.close();
  context.close();

  return EXIT_SUCCESS;
}

/**
 * List all queries on the server.
 */
bool list_queries(std::string host, uint16_t port) {

  /* Connect to server. */
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_REQ);
  std::string addr = "tcp://" + host + ":" + std::to_string(port);
  socket.connect(addr.c_str());

  /* Pack message and send to server. */
  CLIENT_COMMAND command = LIST_QUERIES;
  zmq::message_t request(sizeof(CLIENT_COMMAND));
  std::memcpy(request.data(), &command, sizeof(CLIENT_COMMAND));
  socket.send(request);

  /* Receive list of queries from server. */
  zmq::message_t reply;
  socket.recv(&reply);

  /* Print our list of query ids. */
  std::cout << "There are the following queries at IoT-DB: " << std::endl;
  size_t begin_elem = 0;
  while (begin_elem < reply.size()) {

    size_t query_id = *(reinterpret_cast<size_t *>((char *)reply.data() + begin_elem));
    size_t file_name_size = *(reinterpret_cast<size_t *>((char *)reply.data() + begin_elem + sizeof(size_t)));

    size_t begin_file_name = begin_elem + 2 * sizeof(size_t);
    char *begin_file_name_ptr = &(((char *)reply.data())[begin_file_name]);
    std::string file_name(begin_file_name_ptr, file_name_size);

    std::cout << "-> " << file_name << " (" << query_id << ")" << std::endl;

    begin_elem += 2 * sizeof(size_t) + file_name.size();
  }

  socket.close();
  context.close();

  return EXIT_SUCCESS;
}
} // namespace IotClient

#ifndef TESTING
int main(int argc, const char *argv[]) {

  using namespace IotClient;

  /* Important Variables with defaults. */
  std::string host = "localhost";
  uint16_t port = MASTER_PORT;

  /* Define program options: connection to server. */
  po::options_description connection_options("Connection Parameters");
  connection_options.add_options()("server_host", po::value<std::string>(),
                                   "Set IoT-DB server host (default: localhost).")(
      "server_port", po::value<uint16_t>(), "Set IoT-DB server port (default: 38938).");

  /* Define program options: client commands. */
  po::options_description command_options("Iot-DB client commands (choose one)");
  command_options.add_options()("help", "Produce this help message.")("add_query", po::value<std::string>(),
                                                                      "Send a query (path to *.cpp-file) to IoT-DB.")(
      "remove_query", po::value<size_t>(), "Remove a query (query id) from IoT-DB.")("list_queries",
                                                                                     "List current queries on IoT-DB.");

  /* All program options for command line. */
  po::options_description commandline_options;
  commandline_options.add(connection_options).add(command_options);

  /* Parse parameters. */
  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, commandline_options), vm);
    po::notify(vm);

    /* Only allow one of the client commands. */
    conflicting_options(vm, "add_query", "remove_query");
    conflicting_options(vm, "add_query", "remove_query");
    conflicting_options(vm, "remove_query", "list_queries");

  } catch (const std::exception &e) {
    std::cerr << "Failure while parsing connection parameters!" << std::endl;
    std::cerr << e.what() << std::endl;
    return EXIT_FAILURE;
  }

  /* Interpret given parameters. */
  if (vm.empty() || vm.count("help")) {
    std::cout << commandline_options << std::endl;
    return EXIT_SUCCESS;
  }

  if (vm.count("server_host")) {
    std::cout << "IoT-DB server host set to " << vm["server_host"].as<std::string>() << "." << std::endl;
  } else {
    std::cout << "IoT-DB server host set to " << host << " (default)." << std::endl;
  }

  if (vm.count("server_port")) {
    std::cout << "IoT-DB server port set to " << vm["server_port"].as<uint16_t>() << "." << std::endl;
  } else {
    std::cout << "IoT-DB server port set to " << port << " (default)." << std::endl;
  }
  std::cout << "------------------------------------------------------------" << std::endl;

  /* Interpret client command. */
  if (vm.count("add_query")) {
    return IotClient::add_query(vm["add_query"].as<std::string>(), host, port);
  }

  if (vm.count("remove_query")) {
    return IotClient::remove_query(vm["remove_query"].as<size_t>(), host, port);
  }

  if (vm.count("list_queries")) {
    return IotClient::list_queries(host, port);
  }

  return EXIT_FAILURE;
}
#endif // TESTING