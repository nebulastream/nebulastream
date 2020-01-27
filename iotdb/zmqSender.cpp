#include <zmq.hpp>
#include <thread>

using namespace std;

//  Sends string as 0MQ string, as multipart non-terminal
inline static bool
s_sendmore (zmq::socket_t & socket, const std::string & string) {

  zmq::message_t message(string.size());
  memcpy (message.data(), string.data(), string.size());

  bool rc = socket.send (message, ZMQ_SNDMORE);
  return (rc);
}

//  Convert string to 0MQ string and send to socket
inline static bool
s_send (zmq::socket_t & socket, const std::string & string, int flags = 0) {

  zmq::message_t message(string.size());
  memcpy (message.data(), string.data(), string.size());

  bool rc = socket.send (message, flags);
  return (rc);
}

int main() {
  //  Prepare our context and publisher
  zmq::context_t context(1);
  zmq::socket_t publisher(context, ZMQ_PUSH);
  publisher.connect("tcp://localhost:5563");

  while (1) {
    //  Write two messages, each with an envelope and content
    s_sendmore(publisher, "A");
    s_send(publisher, "We don't want to see this");
    s_sendmore(publisher, "B");
    s_send(publisher, "We would like to see this");
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  return 0;
}

