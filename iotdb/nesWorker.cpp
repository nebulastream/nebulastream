/********************************************************
 *     _   _   ______    _____
 *    | \ | | |  ____|  / ____|
 *    |  \| | | |__    | (___
 *    | . ` | |  __|    \___ \     Worker
 *    | |\  | | |____   ____) |
 *    |_| \_| |______| |_____/
 *
 ********************************************************/

#include <iostream>

#include <Actors/ActorWorker.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Actors/Configurations/ActorWorkerConfig.hpp>

#include "caf/all.hpp"
#include "caf/io/all.hpp"

using std::cout;
using std::endl;
using std::string;

using namespace caf;
using namespace iotdb;

/**
* @brief main method to run the worker with an interactive console command list
*/
void start_worker(actor_system &system, const ActorWorkerConfig &cfg) {
  // keeps track of requests and tries to reconnect on server failures
  auto usage = [] {
    cout << "Usage:" << endl
         << "  quit                  : terminates the program" << endl
         << "  connect <host> <port> : connects to a remote actor" << endl
         << endl;
  };
  usage();

  infer_handle_from_class_t<iotdb::ActorWorker> client;

  for (int i = 1; i <= 5; i++) {
    client = system.spawn<iotdb::ActorWorker>(cfg.ip, cfg.publish_port, cfg.receive_port,
                                              cfg.sensor_type + std::to_string(i));
    if (!cfg.host.empty() && cfg.publish_port > 0)
      anon_send(client, connect_atom::value, cfg.host, cfg.publish_port);
    else
      cout << "*** no server received via config, "
           << R"(please use "connect <host> <port>" before using the calculator)"
           << endl;
  }

  bool done = false;
  client = system.spawn<iotdb::ActorWorker>(cfg.ip, cfg.publish_port, cfg.receive_port, cfg.sensor_type);
  if (!cfg.host.empty() && cfg.publish_port > 0)
    anon_send(client, connect_atom::value, cfg.host, cfg.publish_port);
  else
    cout << "*** no server received via config, "
         << R"(please use "connect <host> <port>" before using the calculator)"
         << endl;

  // defining the handler outside the loop is more efficient as it avoids
  // re-creating the same object over and over again
  message_handler eval(
      [&](const string &cmd) {
        if (cmd != "quit")
          return;
        anon_send_exit(client, exit_reason::user_shutdown);
        done = true;
      },
      [&](string &arg0, string &arg1, string &arg2) {
        if (arg0 == "connect") {
          char *end = nullptr;
          auto lport = strtoul(arg2.c_str(), &end, 10);
          if (end != arg2.c_str() + arg2.size())
            cout << R"(")" << arg2 << R"(" is not an unsigned integer)" << endl;
          else if (lport > std::numeric_limits<uint16_t>::max())
            cout << R"(")" << arg2 << R"(" > )"
                 << std::numeric_limits<uint16_t>::max() << endl;
          else
            anon_send(client, connect_atom::value, move(arg1),
                      static_cast<uint16_t>(lport));
        }
      }
  );
  // read next line, split it, and feed to the eval handler
  string line;
  while (!done && std::getline(std::cin, line)) {
    line = trim(std::move(line)); // ignore leading and trailing whitespaces
    std::vector<string> words;
    split(words, line, is_any_of(" "), token_compress_on);
    if (!message_builder(words.begin(), words.end()).apply(eval))
      usage();
  }
}

void setupLogging() {
  // create PatternLayout
  log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

  // create FileAppender
  LOG4CXX_DECODE_CHAR(fileName, "iotdb.log");
  log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

  // create ConsoleAppender
  log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

  // set log level
  iotdb::logger->setLevel(log4cxx::Level::getDebug());
  // add appenders and other will inherit the settings
  iotdb::logger->addAppender(file);
  iotdb::logger->addAppender(console);
}

void caf_main(actor_system &system, const ActorWorkerConfig &cfg) {
  log4cxx::Logger::getLogger("IOTDB")->setLevel(log4cxx::Level::getDebug());
  setupLogging();
  start_worker(system, cfg);
}

CAF_MAIN(io::middleman)