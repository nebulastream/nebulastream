
#ifndef IOTDB_IMPL_REST_RESTSERVER_H_
#define IOTDB_IMPL_REST_RESTSERVER_H_

namespace iotdb {

/**
 * @brief : This class is responsible for starting the REST server.
 */

class RestServer {

 public:
  bool start(std::string host, u_int16_t port);
};

}

#endif //IOTDB_IMPL_REST_RESTSERVER_H_
