#include <Util/GPUKernnelWrapper/SimpleKernel.cuh>

__global__ void simpleAdditionKernel(const InputRecord* recordValue, const int64_t count, InputRecord* result) {
    auto i = blockIdx.x * blockDim.x + threadIdx.x;

    if (i < count) {
        result[i].test$id = recordValue[i].test$id;
        result[i].test$one = recordValue[i].test$one;
        result[i].test$value = recordValue[i].test$value + 42;
    }
}

void SimpleKernelWrapper::execute(int64_t numberOfTuple, InputRecord* record) {
    // allocate GPU memory to work with the record
    InputRecord* d_record;
    cudaMalloc(&d_record, numberOfTuple * sizeof(InputRecord));

    // copy the record to the GPU memory
    cudaMemcpy(d_record, record, numberOfTuple * sizeof(InputRecord), cudaMemcpyHostToDevice);

    // prepare kernel launch configuration
    dim3 dimBlock(1024, 1, 1);// using 1D kernel, vx * vy * vz must be <= 1024
    dim3 dimGrid((numberOfTuple + dimBlock.x - 1) / dimBlock.x, 1, 1);

    // allocate GPU memory to store the result
    InputRecord* d_result;
    cudaMalloc(&d_result, numberOfTuple * sizeof(InputRecord));

    // launch the kernel
    simpleAdditionKernel<<<dimGrid, dimBlock>>>(d_record, numberOfTuple, d_result);

    // copy the result back to host record
    cudaMemcpy(record, d_result, numberOfTuple * sizeof(InputRecord), cudaMemcpyDeviceToHost);
}
