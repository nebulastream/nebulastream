/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <WorkQueues/AsyncRequestExecutor.hpp>
#include <WorkQueues/RequestTypes/AbstractRequest.hpp>
#include <Util/ThreadNaming.hpp>

//NES::Experimental::AsyncRequestExecutor::AsyncRequestExecutor(uint32_t numOfThreads, StorageHandlerPtr storageHandler) : running(true), storageHandler(std::move(storageHandler)) {
//    for (uint32_t i = 0; i < numOfThreads; ++i) {
//        std::promise<bool> promise;
//        completionFutures.emplace_back(promise.get_future());
//        //todo: is there a reason why the task implementation was passing ptrs to promises?
//        auto thread = std::thread([this, i, promise = std::move(promise)]() mutable {
//          try {
//              setThreadName("RequestExecThr-%d", i);
//              runningRoutine();
//              promise.set_value(true);
//          } catch (std::exception const& ex) {
//              promise.set_exception(std::make_exception_ptr(ex));
//          }
//        });
//        thread.detach();
//    }
//}
//
//
//void NES::Experimental::AsyncRequestExecutor::runningRoutine() {
//    while (true) {
//        std::unique_lock lock(workMutex);
//        while (asyncRequestQueue.empty()) {
//            cv.wait(lock);
//        }
//        if (running) {
//            AbstractRequestPtr abstractRequest = asyncRequestQueue.front();
//            asyncRequestQueue.pop_front();
//            lock.unlock();
//
//            //todo: call error handling inside execute or here?
//
//            //todo: make execute function accept ref to const ptr instead of the deref here?
//            auto handler = *storageHandler;
//            //todo: to avaid the abstract type trouble, we either need to use a template or make execute accept pointers
//            abstractRequest->execute(handler);
//
//            //todo: allow execute to return follow up reqeust or execute the follow up as part of execute?
//        } else {
//            break;
//        }
//    }
//}