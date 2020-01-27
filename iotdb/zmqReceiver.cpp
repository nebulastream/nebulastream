#include <zmq.hpp>
#include <iostream>

using namespace std;

//  Receive 0MQ string from socket and convert into C string
//  Caller must free returned string.
inline static char *
s_recv(void *socket, int flags = 0) {
  zmq_msg_t message;
  zmq_msg_init(&message);

  int rc = zmq_msg_recv(&message, socket, flags);

  if (rc < 0)
    return nullptr;           //  Context terminated, exit

  size_t size = zmq_msg_size(&message);
  char *string = (char *) malloc(size + 1);
  memcpy(string, zmq_msg_data(&message), size);
  zmq_msg_close(&message);
  string[size] = 0;
  return (string);
}

//  Receive 0MQ string from socket and convert into string
inline static std::string
s_recv(zmq::socket_t &socket, int flags = 0) {

  zmq::message_t message;
  socket.recv(&message, flags);

  return std::string(static_cast<char *>(message.data()), message.size());
}

int main() {
  //  Prepare our context and subscriber
  zmq::context_t context(1);
  zmq::socket_t subscriber(context, ZMQ_PULL);
  subscriber.bind("tcp://*:5563");

  while (1) {
    //  Read envelope with address
    std::string address = s_recv(subscriber);
    //  Read message contents
    std::string contents = s_recv(subscriber);

    std::cout << "[" << address << "] " << contents << std::endl;
  }
  return 0;
}