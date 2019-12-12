
#ifndef IOTDB_INCLUDE_ACTORS_CAFSERVER_H_
#define IOTDB_INCLUDE_ACTORS_CAFSERVER_H_

#include <iostream>

namespace iotdb {

/**
 * @brief : This class is responsible for starting the CAF server with coordinator services.
 */

  class CAFServer {

    public:
      bool start();

    private:
      void setupLogging();
  };

}

#endif
