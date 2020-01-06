
#include <iostream>
//#include <Util/Logger.hpp>
namespace iotdb {

void printStackTrace(std::ostream& out);
void exit(int status) __attribute__((noreturn));

#define IOTDB_WARNING_DEPRECATED(X)                                                                                               \
    {                                                                                                                  \
        std::cout << "WARNING: " << X << ": In " << __PRETTY_FUNCTION__ << std::endl;                                  \
        std::cout << "In File: " << __FILE__ << " Line: " << __LINE__ << std::endl;                                    \
    }

#define IOTDB_FATAL_DEPRECATED(X)                                                                                           \
    {                                                                                                                  \
        std::cout << "FATAL ERROR: " << X << ": In " << __PRETTY_FUNCTION__ << std::endl;                              \
        std::cout << "In File: " << __FILE__ << " Line: " << __LINE__ << std::endl;                                    \
        std::cout << "Aborting Execution..." << std::endl;                                                             \
        iotdb::printStackTrace(std::cout);                                                                             \
        iotdb::exit(-1);                                                                                               \
    }

//#define IOTDB_NOT_IMPLEMENTED IOTDB_FATAL_ERROR("Function Not Implemented!")

} // namespace iotdb
