#ifndef NES_SIMPLEKERNEL_CUH
#define NES_SIMPLEKERNEL_CUH

#include <cstdint>

struct InputRecord {
    int64_t test$id;
    int64_t test$one;
    int64_t test$value;
};

class SimpleKernelWrapper {
  public:
    SimpleKernelWrapper()= default;;

    void execute(int64_t numberOfTuple, InputRecord* d_record, InputRecord* d_result);
};

#endif//NES_SIMPLEKERNEL_CUH
