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

    /**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param ptr to NodeEngine
     * @param queryId
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
    static bool checkCompleteOrTimeout(NodeEnginePtr ptr, std::string queryId, size_t expectedResult) {
        size_t timeoutInSec = std::chrono::seconds(5).count();
        const auto p0 = std::chrono::time_point<std::chrono::system_clock>{};
        size_t now = std::chrono::system_clock::to_time_t(p0);

        while (now < now + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NodeEnginePtr")
            if (ptr->getQueryStatistics(queryId)->getProcessedTuple() == expectedResult
                && ptr->getQueryStatistics(queryId)->getProcessedTasks() == expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct")
                return true;
            }
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
        size_t timeoutInSec = std::chrono::seconds(5).count();
        const auto p0 = std::chrono::time_point<std::chrono::system_clock>{};
        size_t now = std::chrono::system_clock::to_time_t(p0);

        while (now < now + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NesWorkerPtr")
            if (ptr->getQueryStatistics(queryId)->getProcessedTuple() == expectedResult
                && ptr->getQueryStatistics(queryId)->getProcessedTasks() == expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct")
                return true;
            }
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
        size_t timeoutInSec = std::chrono::seconds(5).count();
        const auto p0 = std::chrono::time_point<std::chrono::system_clock>{};
        size_t now = std::chrono::system_clock::to_time_t(p0);

        while (now < now + timeoutInSec) {
            NES_DEBUG("checkCompleteOrTimeout: check result NesCoordinatorPtr")
            if (ptr->getQueryStatistics(queryId)->getProcessedTuple() == expectedResult
                && ptr->getQueryStatistics(queryId)->getProcessedTasks() == expectedResult) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct")
                return true;
            }
            sleep(1);
        }
        NES_DEBUG("checkCompleteOrTimeout: expected results are not reached after timeout")
        return false;
    }
};
}
#endif //NES_INCLUDE_UTIL_TESTUTILS_HPP_
