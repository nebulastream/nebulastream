

#include <stdlib.h>
#include <iostream>
#include <cstdlib>


namespace iotdb {

  void printStackTrace(std::ostream& out){

  }

  void exit(int status){
    if (status != EXIT_SUCCESS) {
  #ifndef __APPLE__
      quick_exit(status);
  #else
      abort();
  #endif
    }
    std::exit(status);
  }


}
