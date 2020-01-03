
#ifndef IOTDB_INCLUDE_ACTORS_CAFSERVER_H_
#define IOTDB_INCLUDE_ACTORS_CAFSERVER_H_

#include "Actors/CoordinatorActor.hpp"
#include <iostream>

namespace iotdb {

/**
 * @brief : This class is responsible for starting the CAF server with coordinator services.
 */

  class CAFServer {

    public:
      bool start(const infer_handle_from_class_t<CoordinatorActor>&);

    private:
//      void setupLogging();
  };

}

#endif
