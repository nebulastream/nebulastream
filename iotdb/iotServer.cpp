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

#include <iostream>
#include <string>
#include <tuple>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <zmq.hpp>

#ifndef MASTER_PORT
#define MASTER_PORT 38938
#endif // MASTER_PORT

namespace fs = boost::filesystem;
namespace po = boost::program_options;

enum CLIENT_COMMAND { ADD_QUERY, REMOVE_QUERY, LIST_QUERIES };

/* Status of Iot_DB server. */
size_t number_client_queries = 0;
std::map<size_t, std::tuple<std::string, std::string>> client_queries;
std::map<size_t, std::tuple<std::string, std::string>>::iterator client_queries_it;

void request_add_query(zmq::socket_t &socket, zmq::message_t &request) {

  /* Get information from request. */
  size_t size_file_name = *(reinterpret_cast<size_t *>((char *)request.data() + sizeof(CLIENT_COMMAND)));
  size_t size_file_content =
      *(reinterpret_cast<size_t *>((char *)request.data() + sizeof(CLIENT_COMMAND) + sizeof(size_t)));

  size_t begin_file_name = sizeof(CLIENT_COMMAND) + 2 * sizeof(size_t);
  char *begin_file_name_ptr = &(((char *)request.data())[begin_file_name]);
  std::string file_name(begin_file_name_ptr, size_file_name);

  size_t begin_file_content = sizeof(CLIENT_COMMAND) + 2 * sizeof(size_t) + size_file_name;
  char *begin_file_content_ptr = &(((char *)request.data())[begin_file_content]);
  std::string file_content(begin_file_content_ptr, size_file_content);

  /* Save new query. */
  client_queries[number_client_queries] = std::tuple<std::string, std::string>(file_name, file_content);

  /* Print some information for admin. */
  std::cout << "Received request with client commmand 'ADD_QUERY'." << std::endl;
  std::cout << "-> added new query '" << file_name << "' with id " << number_client_queries << "." << std::endl;

  /* Reply to request with query_id. */
  zmq::message_t reply(sizeof(size_t));
  memcpy(reply.data(), &number_client_queries, sizeof(size_t));
  socket.send(reply);

  /* Increment query id for next query. */
  number_client_queries++;
}

void request_remove_query(zmq::socket_t &socket, zmq::message_t &request) {

  /* Interpret remove query request. */
  size_t query_id = *(reinterpret_cast<size_t *>((char *)request.data() + sizeof(CLIENT_COMMAND)));

  bool removed = false;
  client_queries_it = client_queries.find(query_id);
  if (client_queries_it != client_queries.end()) {
    removed = true;
    client_queries.erase(client_queries_it);
  }

  /* Print some information. */
  std::cout << "Received request with client commmand 'REMOVE_QUERY'." << std::endl;
  if (removed) {
    std::cout << "-> query with id " << query_id << " removed." << std::endl;
  } else {
    std::cout << "-> query with id " << query_id << " not found and not removed." << std::endl;
  }

  /* Reply to request with query_id. */
  zmq::message_t reply(sizeof(bool));
  memcpy(reply.data(), &removed, sizeof(bool));
  socket.send(reply);
}

void request_list_queries(zmq::socket_t &socket, zmq::message_t &request) {

  std::cout << "Received request with client commmand 'LIST_QUERIES'." << std::endl;

  /* Pack message and send to client. */
  size_t message_size = 0;
  for (auto const &query : client_queries) {
    // query id, size of file_name and file_name
    message_size += 2 * sizeof(size_t) + std::get<0>(query.second).size();
  }
  zmq::message_t reply(message_size);

  size_t begin_elem = 0;
  for (auto const &query : client_queries) {
    std::string file_name = std::get<0>(query.second);
    size_t file_name_size = file_name.size();
    std::memcpy((char *)reply.data() + begin_elem, &query.first, sizeof(size_t));
    std::memcpy((char *)reply.data() + begin_elem + sizeof(size_t), &file_name_size, sizeof(size_t));
    std::memcpy((char *)reply.data() + begin_elem + 2 * sizeof(size_t), file_name.data(), file_name_size);
    begin_elem += 2 * sizeof(size_t) + file_name.size();
  }
  socket.send(reply);
}

int main(int argc, const char *argv[]) {

  /* Prepare ZeroMQ context and socket. */
  uint16_t port = MASTER_PORT;
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_REP);
  std::string addr = "tcp://*:" + std::to_string(port);
  socket.bind(addr.c_str());

  while (true) {
    zmq::message_t request;

    /* Wait for the next request. */
    socket.recv(&request);

    /* Interpret client command. */
    CLIENT_COMMAND *command = reinterpret_cast<CLIENT_COMMAND *>(request.data());
    if (*command == ADD_QUERY) {

      request_add_query(socket, request);

    } else if (*command == REMOVE_QUERY) {

      request_remove_query(socket, request);

    } else if (*command == LIST_QUERIES) {

      request_list_queries(socket, request);

    } else {
      std::cerr << "Received request with undefined client command: '" << *command << "'." << std::endl;
    }
  }

  return EXIT_SUCCESS;
}
