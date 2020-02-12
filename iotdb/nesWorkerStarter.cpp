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
  cout << "start with port=" << atoi(argv[1]) << endl;
  wrk->start(/**blocking*/ true, atoi(argv[1]));
  cout << "connect" << endl;
  wrk->connect();
  cout << "worker started" << endl;
}
