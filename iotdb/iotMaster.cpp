#include <Topology/FogTopologyManager.hpp>
#include <API/InputQuery.hpp>
#include <API/Config.hpp>
#include <API/Schema.hpp>

#include "include/API/InputQuery.hpp"
#include <Optimizer/FogOptimizer.hpp>
#include <Optimizer/FogRunTime.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Runtime/CompiledDummyPlan.hpp>
#include <Util/Logger.hpp>

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include <boost/serialization/base_object.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/serialization/list.hpp>
#include <boost/serialization/assume_abstract.hpp>

using namespace iotdb;

void printWelcome()
{
	std::vector<std::string> logo;
	logo.push_back(R"( __         __)");
	logo.push_back(R"(/  \.-"""-./  \)");
	logo.push_back(R"(\   - DFKI -  /)");
	logo.push_back(R"( |   o   o   |)");
	logo.push_back(R"( \  .-'''-.  /)");
	logo.push_back(R"(  '-\__Y__/-')");
	logo.push_back(R"(     `---`)");

	std::cout << "Welcome to the Grizzly Streaming Compiler ʕ•ᴥ•ʔ" << std::endl;
	for(auto a : logo)
		std::cout << a << std::endl;

}
InputQueryPtr createTestQuery()
{
	 Config config = Config::create().
		                  withParallelism(1).
		                  withPreloading().
		                  withBufferSize(1000).
		                  withNumberOfPassesOverInput(1);

	  Schema schema = Schema::create().addField("",INT32);

	  /** \brief create a source using the following functions:
	  * const DataSourcePtr createTestSource();
	  * const DataSourcePtr createBinaryFileSource(const Schema& schema, const std::string& path_to_file);
	  * const DataSourcePtr createRemoteTCPSource(const Schema& schema, const std::string& server_ip, int port);
  */
	DataSourcePtr source = createTestSource();

	InputQueryPtr ptr = std::make_shared<InputQuery>(InputQuery::create(config, source)
		      .filter(PredicatePtr())
		      .printInputQueryPlan());

	return ptr;
}

InputQueryPtr createYSBTestQuery()
{
	 Config config = Config::create().
		                  withParallelism(1).
		                  withPreloading().
		                  withBufferSize(1000).
		                  withNumberOfPassesOverInput(1);

//	  Schema schema = Schema::create().addField("",INT32);

	  /** \brief create a source using the following functions:
	  * const DataSourcePtr createTestSource();
	  * const DataSourcePtr createBinaryFileSource(const Schema& schema, const std::string& path_to_file);
	  * const DataSourcePtr createRemoteTCPSource(const Schema& schema, const std::string& server_ip, int port);
  */
	DataSourcePtr source = createYSBSource(100,10, /*pregen*/ false);

	Schema schema = Schema::create()
		.addField("user_id", 16)
		.addField("page_id", 16)
		.addField("campaign_id", 16)
		.addField("event_type", 16)
		.addField("ad_type", 16)
		.addField("current_ms", UINT64)
		.addField("ip", INT32);

//	.filter(Equal("event_type", "view"))                // filter by event type
//	.window(TumblingProcessingTimeWindow(Counter(100))) // tumbling window of 100 elements
//	.groupBy("campaign_id")                             // group by campaign id
//	.aggregate(Count())                                 // count results per key and window
//	.write("output.csv");
	InputQueryPtr ptr = std::make_shared<InputQuery>(InputQuery::create(config, source)
		      .filter(PredicatePtr())
//			  .window(WindowPtr())
		      .printInputQueryPlan());

	return ptr;
}

CompiledDummyPlanPtr createDummyQEP()
{
	CompiledDummyPlanPtr qep(new CompiledDummyPlan());
	return qep;
}

//CompiledTestQueryExecutionPlanPtr createQEP()
//{
//	CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
//	return qep;
//}
//
//CompiledYSBTestQueryExecutionPlanPtr createYSBQEP()
//{
//	CompiledYSBTestQueryExecutionPlanPtr qep(new CompiledYSBTestQueryExecutionPlan());
//    return qep;
//}

void createTestTopo(FogTopologyManager& fMgnr)
{
	FogTopologyWorkerNodePtr f1 = fMgnr.createFogWorkerNode();

	FogTopologySensorNodePtr s1 = fMgnr.createFogSensorNode();

	FogTopologyLinkPtr l1 = fMgnr.createFogTopologyLink(s1, f1);

	fMgnr.printTopologyPlan();
}

NodeEnginePtr createTestNode()
{
	NodeEnginePtr node = std::make_shared<NodeEngine>(1);
	JSON props = node->getNodeProperties();
	node->printNodeProperties();
	return node;
}

void setupLogging()
{
	 // create PatternLayout
	log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

	// create FileAppender
	LOG4CXX_DECODE_CHAR(fileName, "iotdb.log");
	log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

	// create ConsoleAppender
	log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

	// set log level
	//logger->setLevel(log4cxx::Level::getTrace());
//	logger->setLevel(log4cxx::Level::getDebug());
	logger->setLevel(log4cxx::Level::getInfo());
//	logger->setLevel(log4cxx::Level::getWarn());
	//logger->setLevel(log4cxx::Level::getError());
//	logger->setLevel(log4cxx::Level::getFatal());

	// add appenders and other will inherit the settings
	logger->addAppender(file);
	logger->addAppender(console);
}
void save_qep(const QueryExecutionPlan* s, const char * filename){
    // make an archive
    std::ofstream ofs(filename);
    boost::archive::text_oarchive oa(ofs);
    oa << s;
}

void restore_qep(QueryExecutionPlan* s, const char * filename)
{
    // open the archive
    std::ifstream ifs(filename);
    boost::archive::text_iarchive ia(ifs);

    // restore the schedule from the archive
    ia >> s;
    std::cout << "numsrc=" << s->getSources().size() << std::endl;
}

int main(int argc, const char *argv[]) {
	log4cxx::Logger::getLogger("IOTDB")->setLevel(log4cxx::Level::getInfo());
	setupLogging();
	std::string filename("");
	filename += "/home/zeuchste/git/IoTDB/iotdb/build/tests/demofile.txt";
	QueryExecutionPlanPtr q = createTestQEP();

	DataSourcePtr src = createYSBSource(100,10, /*pregen*/ false);
	q->addDataSource(src);

	std::cout << "qep before:" << std::endl;
	q->print();
	save_qep(q.get(), filename.c_str());

	std::cout << "save finished:" << std::endl;

//	QueryExecutionPlanPtr q2 = createTestQEP();
	QueryExecutionPlan* q2 = new QueryExecutionPlan();

    std::ifstream ifs(filename);
    boost::archive::text_iarchive ia(ifs);
    ia >> q2;
    std::cout << "numsrc=" << q2->getSources().size() << std::endl;
	std::cout << "qep afterwards:" << std::endl;
	q2->print();

	return 0;
	printWelcome();
	FogTopologyManager& fMgnr = FogTopologyManager::getInstance();
	createTestTopo(fMgnr);

	//normal test query
	InputQueryPtr query = createTestQuery();
	CompiledDummyPlanPtr qep = createDummyQEP();

	//YSB Query
//	InputQueryPtr query = createYSBTestQuery();
//	CompiledYSBTestQueryExecutionPlanPtr qep = createYSBQEP();
//	qep->setDataSource(query->getSource());
	//skipping LogicalPlanManager

	FogOptimizer& fogOpt = FogOptimizer::getInstance();
	FogExecutionPlanPtr execPlan = fogOpt.map(query, fMgnr.getTopologyPlan());
	fogOpt.optimize(execPlan);//TODO: does nothing atm

	FogRunTime& runtime = FogRunTime::getInstance();
	NodeEnginePtr nodePtr = createTestNode();
	runtime.registerNode(nodePtr);

	runtime.deployQuery(qep);

}
