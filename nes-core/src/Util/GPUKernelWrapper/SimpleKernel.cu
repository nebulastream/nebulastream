#include <Util/GPUKernnelWrapper/SimpleKernel.cuh>

__global__ void simpleAdditionKernel(const InputRecord* recordValue, const int64_t count, InputRecord* result) {
    auto i = blockIdx.x * blockDim.x + threadIdx.x;

    if (i < count) {
        result[i].test$id = recordValue[i].test$id;
        result[i].test$one = recordValue[i].test$one;
        result[i].test$value = recordValue[i].test$value + 42;
    }
}

void SimpleKernelWrapper::execute(int64_t numberOfTuple, InputRecord* d_record, InputRecord* d_result) {
    // prepare kernel launch configuration
    dim3 dimBlock(1024, 1, 1);// using 1D kernel, vx * vy * vz must be <= 1024
    dim3 dimGrid((numberOfTuple + dimBlock.x - 1) / dimBlock.x, 1, 1);

    // launch the kernel
    simpleAdditionKernel<<<dimGrid, dimBlock>>>(d_record, numberOfTuple, d_result);
}
