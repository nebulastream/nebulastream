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
#include <Util/UtilityFunctions.hpp>
#include "boost/program_options.hpp"
#include <Util/Logger.hpp>
#include <Components/NesWorker.hpp>

using std::cout;
using std::endl;
using std::string;
namespace po = boost::program_options;

using namespace caf;
using namespace NES;

void runningCLI(NesWorkerPtr wrk) {
  auto usage = [] {
    cout << "Usage:" << endl
    << "  quit                  : terminates the program" << endl
    << "  connect <host> <port> : connects to a remote actor" << endl
    << "  register_sensor "
    "<sourceType (OneGeneratorSource||CSVSource)> " "<sourceConf (1..n|filePath)> "
    "<physical stream name> " "<logical stream name> " ":register a physical stream"
    << "  register_stream " "<logical stream name> " "<pathToSchemaFile> " ": register a logical stream"
    << endl;
  };

  bool done = false;
  // defining the handler outside the loop is more efficient as it avoids
  // re-creating the same object over and over again
  message_handler eval(
      [&](const string &cmd) {
        if (cmd != "quit") {
          wrk->disconnect();
        }
        done = true;
      }
      ,
      [&](string &arg0, string &arg1, string &arg2) {
        if (arg0 == "connect") {
          char *end = nullptr;
          auto lport = strtoul(arg2.c_str(), &end, 10);
          if (end != arg2.c_str() + arg2.size()) {
            cout << R"(")" << arg2 << R"(" is not an unsigned integer)" << endl;
          } else if (lport > std::numeric_limits<uint16_t>::max()) {
            cout << R"(")" << arg2 << R"(" > )"
            << std::numeric_limits<uint16_t>::max() << endl;
          } else {
            wrk->connect(arg1, lport);
          }
        } else if (arg0 == "register_stream") {
          cout << "NESWorker: register logical stream: stream name=" << arg1
          << " filePath=" << arg2;
          wrk->registerLogicalStream(arg1, arg2);
        }

      }
      ,
      [&](string &arg0, string &arg1, string &arg2, string &arg3,
          string &arg4) {
        if (arg0 == "register_sesnor") {
          cout << "NESWorker: register physical stream: source type=" << arg1
          << " sourceConf=" << arg2 << " physical stream name=" << arg3
          << " logical stream name=" << arg4 << endl;
        }
        wrk->registerPhysicalStream(arg1, arg2, arg3, arg4);
      }
  );
  // read next line, split it, and feed to the eval handler
  string line;
  while (!done && std::getline(std::cin, line)) {
    line = NES::UtilityFunctions::trim(std::move(line));  // ignore leading and trailing whitespaces
    std::vector<string> words;
    split(words, line, is_any_of(" "), token_compress_on);
    if (!message_builder(words.begin(), words.end()).apply(eval))
    usage();
  }
}
static void setupLogging() {
// create PatternLayout
  log4cxx::LayoutPtr layoutPtr(
      new log4cxx::PatternLayout(
          "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

// create FileAppender
  LOG4CXX_DECODE_CHAR(fileName, "iotdb.log");
  log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

// create ConsoleAppender
  log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

// set log level
  NES::NESLogger->setLevel(log4cxx::Level::getDebug());
// add appenders and other will inherit the settings
  NES::NESLogger->addAppender(file);
  NES::NESLogger->addAppender(console);
}

int main(int argc, char **argv) {
  setupLogging();

  namespace po = boost::program_options;
  po::options_description desc("Options");

  desc.add_options()("help", "Print help messages");

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << "Basic Command Line Parameter " << std::endl << desc
              << std::endl;
    return 0;
  }
  NesWorkerPtr wrk = std::make_shared<NesWorker>();
  runningCLI(wrk);
}
