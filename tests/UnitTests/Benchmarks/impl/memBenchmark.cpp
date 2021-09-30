//#include <omp.h>
#include <cstring>
#include <chrono>
#include <iostream>
#include <vector>
#include <omp.h>
#include <numa.h>
using namespace std;
//int main(int argc, char* argv[]) {

void runBenchmark(size_t dataSetSizeInBytes, size_t bufferSizeInBytes) {

    size_t elapsedTime = 0;
    size_t workerCnt = 0;
    //    num_threads(workerCnt)
#pragma omp parallel reduction(+:elapsedTime) reduction(+:workerCnt)
    {
        //                auto i = omp_get_thread_num();
        //    for (size_t i = 0; i < maxWorkNum; i++) {
        workerCnt++;
        int thread_num = omp_get_thread_num();
        int cpu_num = sched_getcpu();
        int node = numa_node_of_cpu(cpu_num);
        printf("Thread %3d is running on CPU %3d on numa node %3d\n", thread_num, cpu_num, node);

        size_t dataSetPerThread = dataSetSizeInBytes/omp_get_num_threads();
        size_t bufferCntPerThread = dataSetPerThread/bufferSizeInBytes;
//        std::cout << " Experiment setup workerCnt=" << omp_get_num_threads()
//                  << " bufferSizeInBytes=" << bufferSizeInBytes
//                  << " GB_Overall=" << dataSetSizeInBytes/1024/1024/1024
//                  << " Buffer_Overall=" << dataSetPerThread/bufferSizeInBytes
//                  << " GB_per_thread=" << dataSetPerThread/1024/1024/1024
//                  << " Buffer_Cnt_per_thread=" << bufferCntPerThread
//                  << std::endl;

        void* dest = malloc(dataSetPerThread);
        void* src = malloc(dataSetPerThread);
        std::memset(dest, 0, dataSetPerThread);
        std::memset(src, 0, dataSetPerThread);
        auto start = std::chrono::high_resolution_clock::now();
        for (int u = 0; u < bufferCntPerThread; u++) {
            memcpy((uint8_t*) src + (u*bufferSizeInBytes),
                   (uint8_t*) dest + (u*bufferSizeInBytes),
                   bufferSizeInBytes
            );
        }//end of for copy
        auto stop = std::chrono::high_resolution_clock::now();
        elapsedTime = std::chrono::duration_cast<std::chrono::microseconds>
            (stop - start).count();
        //                std::cout << " thread i=" << i << " time=" << elapsedTime << endl;
        auto res = std::memcmp(dest, src, dataSetPerThread);
        if (res) {
            std::cout << "Error in compare" <<
                      std::endl;
            exit(1);
        }
        free(src);
        free(dest);
    }

    double elTimeInSec = double(elapsedTime)/double(workerCnt);
    elTimeInSec = elTimeInSec/1000/1000;
//    double elaInSec = (double) elapsedTime/1000/1000;
    double sizeInGB = (double) dataSetSizeInBytes/1024/1024/1024;
    double bw = sizeInGB/elTimeInSec;
    std::cout << "runtime with bufferSIze=" << bufferSizeInBytes
              << " workerCnt=" << workerCnt
              << " elapsed sec=" << elTimeInSec
              << " sizeInGB=" << sizeInGB
              << " bandwidth="
              << bw << " GB/s" <<
              std::endl;
}

int main() {
    //
    //    for (int i = 0; i < argc; ++i)
    //        std::cout << "i=" << i << argv[i] << "\n";
    //
    //    vector workerCnts = {1, 2, 4, 8};
    //    vector workerCnts = {8};
    //    size_t bufferSizeInBytes = 1024;
    uint64_t dataSetSizeInBytes = (uint64_t) 32*(uint64_t) 1024*(uint64_t) 1024*(uint64_t) 1024; //
    //    vector bufferSizesInBytes = {1024, 65536, 131072, 655360, 1310720, 13107200, 131072000}; //1KB, 64KB, 128KB, 512KB, 1MB, 10MB, 100MB
    //    vector bufferSizesInBytes = {1310720}; //1KB, 64KB, 128KB, 512KB, 1MB, 10MB, 100MB
    const char* env_p = std::getenv("OMP_NUM_THREADS");
    std::cout << "t=" << env_p << std::endl;
    runBenchmark(dataSetSizeInBytes, 1310720);

    //    for (size_t workerCnt : workerCnts) {
    //        for (size_t bufferSizeInBytes : bufferSizesInBytes) {
    //            runBenchmark(dataSetSizeInBytes, bufferSizeInBytes, workerCnt);
    //        }
    //    }
}