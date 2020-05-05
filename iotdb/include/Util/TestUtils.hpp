#ifndef NES_INCLUDE_UTIL_TESTUTILS_HPP_
#define NES_INCLUDE_UTIL_TESTUTILS_HPP_
#include <iostream>
#include <memory>
#include <NodeEngine/NodeEngine.hpp>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>
using namespace std;

namespace NES {

class TestUtils {
  public:
    static bool checkCompleteOrTimeout(NodeEnginePtr ptr, std::string queryId, size_t expectedResult) {
        size_t timeoutInSec = 5;
        size_t now = time(0);

        while (time(0) < now + timeoutInSec) {
            cout << "check result NodeEnginePtr" << endl;
            if (ptr->getNumberOfProcessedBuffer(queryId) == expectedResult
                && ptr->getNumberOfProcessedTasks(queryId) == expectedResult) {
                cout << "results are correct" << endl;
                return true;
            }
            sleep(1);
        }
        cout << "expected results are not reached after timeout" << endl;
        return false;
    }

    static bool checkCompleteOrTimeout(NesWorkerPtr ptr, std::string queryId, size_t expectedResult) {
        size_t timeoutInSec = 5;
        size_t now = time(0);

        while (time(0) < now + timeoutInSec) {
            cout << "check result NesWorkerPtr" << endl;
            if (ptr->getNumberOfProcessedBuffer(queryId) == expectedResult
                && ptr->getNumberOfProcessedTasks(queryId) == expectedResult) {
                cout << "results are correct" << endl;
                return true;
            }
            sleep(1);
        }
        cout << "expected results are not reached after timeout" << endl;
        return false;
    }

    static bool checkCompleteOrTimeout(NesCoordinatorPtr ptr, std::string queryId, size_t expectedResult) {
        size_t timeoutInSec = 5;
        size_t now = time(0);

        while (time(0) < now + timeoutInSec) {
            cout << "check result NesCoordinatorPtr" << endl;
            if (ptr->getNumberOfProcessedBuffer(queryId) == expectedResult
                && ptr->getNumberOfProcessedTasks(queryId) == expectedResult) {
                cout << "results are correct" << endl;
                return true;
            }
            sleep(1);
        }
        cout << "expected results are not reached after timeout" << endl;
        return false;
    }


};
}
#endif //NES_INCLUDE_UTIL_TESTUTILS_HPP_
