/**
 *
 * In this file the server side of the IoT-DB is implemented.
 * With this program the DBMS is able to manage queries from the user.
 *
 * Tasks:
 *   - Receive messages from users on a specified port.
 *   - Process the commands from the users:
 *     - Receive a query (*.cpp) from the user, send query id back
 *     - Send a list of current queries on the server
 *     - Remove a query from the server
 *
 */

#include <csignal>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <tuple>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <zmq.hpp>

#ifndef MASTER_PORT
#define MASTER_PORT 38938
#endif // MASTER_PORT

namespace IotServer {

namespace fs = boost::filesystem;
namespace po = boost::program_options;

enum CLIENT_COMMAND { ADD_QUERY, REMOVE_QUERY, LIST_QUERIES };
using client_query_t = std::pair<size_t, std::tuple<std::string, std::string>>;
using client_queries_t = std::map<size_t, std::tuple<std::string, std::string>>;

std::shared_ptr<zmq::message_t> add_query_request(std::shared_ptr<zmq::message_t> request,
                                                  std::shared_ptr<client_queries_t> client_queries,
                                                  std::shared_ptr<size_t> client_queries_id) {

  /* Get string sizes from request. */
  size_t size_file_name = *(reinterpret_cast<size_t *>((char *)request->data() + sizeof(CLIENT_COMMAND)));
  size_t size_file_content =
      *(reinterpret_cast<size_t *>((char *)request->data() + sizeof(CLIENT_COMMAND) + sizeof(size_t)));

  /* Get file name string from request. */
  size_t begin_file_name = sizeof(CLIENT_COMMAND) + 2 * sizeof(size_t);
  char *begin_file_name_ptr = &(((char *)request->data())[begin_file_name]);
  std::string file_name(begin_file_name_ptr, size_file_name);

  /* Get file content from request. */
  size_t begin_file_content = sizeof(CLIENT_COMMAND) + 2 * sizeof(size_t) + size_file_name;
  char *begin_file_content_ptr = &(((char *)request->data())[begin_file_content]);
  std::string file_content(begin_file_content_ptr, size_file_content);

  /* Save new query. */
  client_queries->insert(
      client_query_t(*client_queries_id, std::tuple<std::string, std::string>(file_name, file_content)));

  /* Print some information for admin. */
  std::cout << "Received request with client commmand 'ADD_QUERY'." << std::endl;
  std::cout << "-> added new query '" << file_name << "' with id " << *client_queries_id << "." << std::endl;
  std::cout << "-> file content:" << std::endl << file_content << std::endl << std::endl;

  /* Reply to request with query_id. */
  std::shared_ptr<zmq::message_t> reply(new zmq::message_t(sizeof(size_t)));
  memcpy(reply->data(), client_queries_id.get(), sizeof(size_t));

  /* Increment query id for next query. */
  (*client_queries_id)++;

  return reply;
}

std::shared_ptr<zmq::message_t> remove_query_request(std::shared_ptr<zmq::message_t> request,
                                                     std::shared_ptr<client_queries_t> client_queries) {

  /* Interpret remove query request. */
  size_t query_id = *(reinterpret_cast<size_t *>((char *)request->data() + sizeof(CLIENT_COMMAND)));

  /* Try to remove from server. */
  bool removed = false;
  auto client_queries_it = client_queries->find(query_id);
  if (client_queries_it != client_queries->end()) {
    removed = true;
    client_queries->erase(client_queries_it);
  }

  /* Print some information for admin. */
  std::cout << "Received request with client commmand 'REMOVE_QUERY'." << std::endl;
  if (removed) {
    std::cout << "-> query with id " << query_id << " removed." << std::endl;
  } else {
    std::cout << "-> query with id " << query_id << " not found and not removed." << std::endl;
  }

  /* Reply to request with query_id. */
  std::shared_ptr<zmq::message_t> reply(new zmq::message_t(sizeof(bool)));
  memcpy(reply->data(), &removed, sizeof(bool));

  return (reply);
}

std::shared_ptr<zmq::message_t> list_queries_request(std::shared_ptr<zmq::message_t> request,
                                                     std::shared_ptr<client_queries_t> client_queries) {

  /* Print some information for admin. */
  std::cout << "Received request with client commmand 'LIST_QUERIES'." << std::endl;

  /* Pack message and send to client. */
  size_t message_size = 0;
  for (auto const &query : *client_queries) {
    // query id, size of file_name and file_name
    message_size += 2 * sizeof(size_t) + std::get<0>(query.second).size();
  }
  std::shared_ptr<zmq::message_t> reply(new zmq::message_t(message_size));

  size_t begin_elem = 0;
  for (auto const &query : *client_queries) {
    std::string file_name = std::get<0>(query.second);
    size_t file_name_size = file_name.size();
    std::memcpy((char *)reply->data() + begin_elem, &query.first, sizeof(size_t));
    std::memcpy((char *)reply->data() + begin_elem + sizeof(size_t), &file_name_size, sizeof(size_t));
    std::memcpy((char *)reply->data() + begin_elem + 2 * sizeof(size_t), file_name.data(), file_name_size);
    begin_elem += 2 * sizeof(size_t) + file_name.size();
  }

  return (reply);
}

void receive_reply(std::shared_ptr<client_queries_t> client_queries, std::shared_ptr<size_t> client_queries_id) {

  /* Prepare ZeroMQ context and socket. */
  uint16_t port = MASTER_PORT;
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_REP);
  std::string addr = "tcp://*:" + std::to_string(port);
  socket.bind(addr.c_str());

  std::cout << "Waiting for commands from a IoT-DB clients on port " << std::to_string(port) << "." << std::endl;
  std::cout << "Press CTRL+C to exit." << std::endl;

  /* Listening loop. */
  while (true) {

    /* Wait for the next request. */
    std::shared_ptr<zmq::message_t> request(new zmq::message_t());
    socket.recv(request.get());

    /* Interpret client command. */
    CLIENT_COMMAND *command = reinterpret_cast<CLIENT_COMMAND *>(request->data());
    if (*command == ADD_QUERY) {

      auto reply = add_query_request(request, client_queries, client_queries_id);
      socket.send(*reply);

    } else if (*command == REMOVE_QUERY) {

      auto reply = remove_query_request(request, client_queries);
      socket.send(*reply);

    } else if (*command == LIST_QUERIES) {

      auto reply = list_queries_request(request, client_queries);
      socket.send(*reply);

    } else {
      std::cerr << "Received request with undefined client command!" << std::endl;
    }
  }
}
} // namespace IotServer

#ifndef TESTING
int main(int argc, const char *argv[]) {

  using namespace IotServer;

  /* Status of Iot-DB server. */
  auto client_queries_id = std::make_shared<size_t>();
  auto client_queries = std::make_shared<IotServer::client_queries_t>();
  *client_queries_id = 0;
  IotServer::receive_reply(client_queries, client_queries_id);

  return EXIT_SUCCESS;
}
#endif // TESTING
