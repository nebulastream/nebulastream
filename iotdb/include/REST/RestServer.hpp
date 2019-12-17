
#ifndef IOTDB_IMPL_REST_RESTSERVER_H_
#define IOTDB_IMPL_REST_RESTSERVER_H_

#include "Actors/CoordinatorActor.hpp"

namespace iotdb {

  /**
   * @brief : This class is responsible for starting the REST server.
   */

  class RestServer {

    public:
      bool start(std::string host, u_int16_t port, infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle);
  };

}

#endif //IOTDB_IMPL_REST_RESTSERVER_H_
