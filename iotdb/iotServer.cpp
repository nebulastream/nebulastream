/**
 *
 * Open Questions:
 * 		- support half-duplex links?
 *		- how to handle the ids among nodes?
 *		- how does a node knows to whom it is connected?
 *		- make a parrent class for fogentries?
 *
 *	Todo:
 *		- add remove columns/row in matrix
 *		- add node ids
 *		- add unit tests
 *		- make TopologyManager Singleton
 *		- add createFogNode/FogSensor
 *		- make it thread safe
 */

#include <iostream>
#include <string>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <zmq.hpp>

//#include <Topology/FogToplogyManager.hpp>

#ifndef MASTER_PORT
#define MASTER_PORT 38938
#endif // MASTER_PORT

namespace fs = boost::filesystem;
namespace po = boost::program_options;

enum CLIENT_COMMAND { ADD_QUERY, REMOVE_QUERY, LIST_QUERIES };

/*
void createTestTopo(FogTopologyManager* fMgnr)
{
        FogToplogyNodePtr f1 = std::make_shared<FogToplogyNode>();
        fMgnr->addFogNode(f1);

        FogToplogySensorPtr s1 = std::make_shared<FogToplogySensor>();
        fMgnr->addSensorNode(s1);

        FogToplogySensorPtr s2 = std::make_shared<FogToplogySensor>();
        fMgnr->addSensorNode(s2);

        fMgnr->addLink(f1->getNodeId(), s1->getSensorId(), NodeToSensor);
        fMgnr->addLink(f1->getNodeId(), s2->getSensorId(), NodeToSensor);

        FogTopologyPlanPtr fPlan = fMgnr->getPlan();

        fPlan->printPlan();
}
*/

int main(int argc, const char *argv[]) {
  // FogTopologyManager* fMgnr = new FogTopologyManager();
  // createTestTopo(fMgnr);

  size_t number_client_queries = 0;
  std::map<size_t, std::string> client_queries;
  std::map<size_t, std::string>::iterator client_queries_it;

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

      /* Interpret add query request. */
      std::string file_content(static_cast<char *>((char *)request.data() + sizeof(CLIENT_COMMAND)), request.size());
      client_queries[number_client_queries] = file_content;

      /* Print some information. */
      std::cout << "Received request with client commmand 'ADD_QUERY'." << std::endl;
      std::cout << "-> added new query with id " << number_client_queries << "." << std::endl;

      /* Reply to request with query_id. */
      zmq::message_t reply(sizeof(size_t));
      memcpy(reply.data(), &number_client_queries, sizeof(size_t));
      socket.send(reply);

      /* Increment query id for next query. */
      number_client_queries++;

    } else if (*command == REMOVE_QUERY) {

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

    } else if (*command == LIST_QUERIES) {

      std::cout << "Received request with client commmand 'LIST_QUERIES'." << std::endl;
      /* Pack message and send to client. */
      zmq::message_t reply(client_queries.size() * sizeof(size_t));
      size_t i = 0;
      for (auto const &query : client_queries) {
        std::memcpy((char *)reply.data() + i * sizeof(size_t), &query.first, sizeof(size_t));
        i++;
      }
      socket.send(reply);

    } else {
      std::cerr << "Received request with undefined client command: '" << *command << "'." << std::endl;
    }
  }

  return EXIT_SUCCESS;
}
