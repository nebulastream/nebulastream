#ifndef NES_INCLUDE_UTIL_TESTUTILS_HPP_
#define NES_INCLUDE_UTIL_TESTUTILS_HPP_
#include <iostream>
#include <memory>
#include <NodeEngine/NodeEngine.hpp>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>
#include <chrono>
using Seconds = std::chrono::seconds;
using Clock = std::chrono::high_resolution_clock;
using namespace std;

namespace NES {

/**
 * @brief this is a util class for the tests
 */
class TestUtils {
  public:
    static const size_t timeout = 10;
    /**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param ptr to NodeEngine
     * @param queryId
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
    static bool checkCompleteOrTimeout(NodeEnginePtr ptr, std::string queryId, size_t expectedResult) {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        auto now = start_timestamp;
        while ((now = std::chrono::system_clock::now()) < start_timestamp + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NodeEnginePtr")
            if (ptr->getQueryStatistics(queryId)->getProcessedBuffers() == expectedResult
                && ptr->getQueryStatistics(queryId)->getProcessedTasks() == expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct")
                return true;
            }
            NES_DEBUG("checkCompleteOrTimeout: sleep because val=" << ptr->getQueryStatistics(queryId)->getProcessedTuple() << " < " << expectedResult)
            sleep(1);
        }
        NES_DEBUG("checkCompleteOrTimeout: expected results are not reached after timeout")
        return false;
    }
    /**
         * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
         * @param ptr to NesWorker
         * @param queryId
         * @param expectedResult
         * @return bool indicating if the expected results are matched
         */
    static bool checkCompleteOrTimeout(NesWorkerPtr ptr, std::string queryId, size_t expectedResult) {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        auto now = start_timestamp;
        while ((now = std::chrono::system_clock::now()) < start_timestamp + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NesWorkerPtr")
            if (ptr->getQueryStatistics(queryId)->getProcessedBuffers() == expectedResult
                && ptr->getQueryStatistics(queryId)->getProcessedTasks() == expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct")
                return true;
            }
            NES_DEBUG("checkCompleteOrTimeout: sleep because val=" << ptr->getQueryStatistics(queryId)->getProcessedTuple() << " < " << expectedResult)
            sleep(1);
        }
        NES_DEBUG("checkCompleteOrTimeout: expected results are not reached after timeout")
        return false;
    }

    /**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param ptr to NesCoordinator
     * @param queryId
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
    static bool checkCompleteOrTimeout(NesCoordinatorPtr ptr, std::string queryId, size_t expectedResult) {
        auto timeoutInSec = std::chrono::seconds(timeout);
        auto start_timestamp = std::chrono::system_clock::now();
        auto now = start_timestamp;
        while ((now = std::chrono::system_clock::now()) < start_timestamp + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NesCoordinatorPtr")
            if (ptr->getQueryStatistics(queryId)->getProcessedBuffers() == expectedResult
                && ptr->getQueryStatistics(queryId)->getProcessedTasks() == expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct")
                return true;
            }
            NES_DEBUG("checkCompleteOrTimeout: sleep because val=" << ptr->getQueryStatistics(queryId)->getProcessedTuple() << " < " << expectedResult)
            sleep(1);
        }
        NES_DEBUG("checkCompleteOrTimeout: expected results are not reached after timeout")
        return false;
    }
};
}
#endif //NES_INCLUDE_UTIL_TESTUTILS_HPP_
