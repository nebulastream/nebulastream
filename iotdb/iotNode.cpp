
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <Util/Logger.hpp>
#include <NodeEngine/NodeEngine.hpp>

using boost::asio::ip::tcp;
using namespace iotdb;
using namespace std; // For strlen.

const size_t serverPort = 5555;
const size_t clientPort = 6666;
enum { max_length = 1024*10 };
enum NODE_COMMANDS {
    START_QUERY = 1,
    STOP_QUERY = 2,
    DEPLOY_QEP = 3 };

boost::asio::io_service io_service;
typedef boost::shared_ptr<tcp::socket> socket_ptr;
std::vector<QueryExecutionPlanPtr> qeps;

void start()
{
    for(auto& q : qeps)
    {
        IOTDB_DEBUG("IOTNODE: register query " << q)
        Dispatcher::instance().registerQuery(q);
    }
    IOTDB_DEBUG("IOTNODE: start thread pool")
    ThreadPool::instance().start();
}

void stop()
{
    for(auto& q : qeps)
    {
        IOTDB_DEBUG("IOTNODE: deregister query " << q)
        Dispatcher::instance().deregisterQuery(q);
    }
    IOTDB_DEBUG("IOTNODE: stop thread pool")
    ThreadPool::instance().stop();
}

bool registerNodeInFog(string host, string port)
{
    IOTDB_DEBUG("IOTNODE: register with server " << host << ":" << port)
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(tcp::v4(), host, port);
    tcp::resolver::iterator iterator = resolver.resolve(query);
    tcp::socket s(io_service);
    s.connect(*iterator);
    IOTDB_DEBUG("connected to " << host << ":" << port << " successfully")

    NodeEnginePtr node = std::make_shared<NodeEngine>();
    JSON props = node->getNodeProperties();

    boost::asio::write(s, boost::asio::buffer(props.dump(), props.dump().size()));
//
    char reply[14];
    boost::asio::read(s, boost::asio::buffer(reply, max_length));
    IOTDB_DEBUG("IOTNODE: Server replyied " << reply)
    if(strcmp(reply, "REG_COMPLETED") == 0)
    {
        IOTDB_DEBUG("IOTNODE: registration successful" << reply)
        return true;
    }
    else
    {
        IOTDB_DEBUG("IOTNODE: registration unsuccessful" << reply)
        return false;
    }
}

void commandProcess(socket_ptr sock)
{
    IOTDB_DEBUG("IOTNODE: process incomming command")
    char data[max_length];
    boost::system::error_code error;
    size_t length = sock->read_some(boost::asio::buffer(data), error);
    assert(length < max_length);
    IOTDB_DEBUG("IOTNODE: received command=" << data)
    if (error)
        throw boost::system::system_error(error);

    char cmd = data[0];

    //first char identifies cmd
    if(cmd == '1')
    {
        IOTDB_DEBUG("IOTNODE: received start query command")
        start();
    }
    else if(cmd == '2')
    {
        IOTDB_DEBUG("IOTNODE: received stop query command")
        stop();
    }
    else if(cmd == '3')
    {
        IOTDB_DEBUG("IOTNODE: received deploy query command")
        std::string qepFile = &data[1];
        IOTDB_DEBUG("qepFile=" << qepFile)
        std::stringstream ifs(qepFile.c_str());
        boost::archive::text_iarchive ia(ifs);
        QueryExecutionPlan* q = new QueryExecutionPlan();
        ia >> q;
        QueryExecutionPlanPtr qPtr = std::make_shared<QueryExecutionPlan>();
        qPtr.reset(q);
        qeps.push_back(qPtr);
        IOTDB_DEBUG("received QEP after deserialization:")
        q->print();
    }
    else
    {
        std::cerr << "COMMAND NOT FOUND" << std::endl;
    }
}

void listen(boost::asio::io_service& io_service, short port){
    IOTDB_DEBUG("IOTNODE: start listener for incoming commands")
    tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), port));
      for (;;)
      {
        socket_ptr sock(new tcp::socket(io_service));
        a.accept(*sock);
        boost::thread t(boost::bind(commandProcess, sock));
      }
}

void setupLogging()
{
     // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "node.log");
    log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

    // set log level
    //logger->setLevel(log4cxx::Level::getTrace());
//  logger->setLevel(log4cxx::Level::getDebug());
    logger->setLevel(log4cxx::Level::getInfo());
//  logger->setLevel(log4cxx::Level::getWarn());
    //logger->setLevel(log4cxx::Level::getError());
//  logger->setLevel(log4cxx::Level::getFatal());

    // add appenders and other will inherit the settings
    logger->addAppender(file);
    logger->addAppender(console);
}

void initNodeEngine()
{
    iotdb::Dispatcher::instance();
    iotdb::BufferManager::instance();
    iotdb::ThreadPool::instance();
    //optional
//    iotdb::BufferManager::instance().setBufferSize(bufferSizeInByte);
//    ThreadPool::instance().setNumberOfThreads(threadCnt);
}


int main(int argc, char* argv[])
{
    setupLogging();
    try
    {
        if (argc != 3)
        {
          std::cerr << "Usage: blocking_tcp_echo_client <host> <port>\n";
          return 1;
        }
        std::string host = argv[1];
        std::string port = argv[2];

        bool successReg = registerNodeInFog(host, port);

        IOTDB_DEBUG("IOTNODE: initialize node engine")
        initNodeEngine();

        if(successReg)
        {
            IOTDB_DEBUG("IOTNODE: waiting for commands")
            boost::asio::io_service io_service;
            listen(io_service, clientPort);
        }
        else
        {
            IOTDB_ERROR("IOTNODE: register failed")
            return -1;
        }
    }//end of try
    catch (std::exception& e)
    {
        IOTDB_ERROR("Exception: " << e.what())
    }

    return 0;
}
