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

#ifndef NES_NES_CORE_INCLUDE_RUNTIME_ASYNCTASKEXECUTOR_HPP_
#define NES_NES_CORE_INCLUDE_RUNTIME_ASYNCTASKEXECUTOR_HPP_

#include <Util/Logger.hpp>
#include <condition_variable>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

namespace NES::Runtime {

class AsyncTaskExecutor {
  public:
    template<class R>
    class AsyncTaskFuture {
      public:
        explicit AsyncTaskFuture(std::shared_ptr<std::promise<R>> promise, AsyncTaskExecutor* owner)
            : promise(std::move(promise)), owner(owner) {
            future = this->promise->get_future();
        }

        AsyncTaskFuture(const AsyncTaskFuture& that) { *this = that; }

        AsyncTaskFuture& operator=(const AsyncTaskFuture& that) {
            promise = that.promise;
            future = that.future;
            return *this;
        }

        R wait() { return future.get(); }

        template<class Function>
        [[nodiscard]] AsyncTaskFuture<std::invoke_result_t<std::decay_t<Function>, std::decay_t<R>>> thenAsync(Function&& f) {
            return owner->runAsync([this, f]() {
                return f(future.get());
            });
        }

        bool operator!() const { return promise == nullptr; }

        operator bool() const { return promise != nullptr; }

      private:
        std::shared_ptr<std::promise<R>> promise;
        std::shared_future<R> future;
        AsyncTaskExecutor* owner;
    };

  private:
    template<class R, class... ArgTypes>
    class AsyncTask {
      public:
        template<class Function,
                 std::enable_if_t<std::is_same_v<R, std::invoke_result_t<std::decay_t<Function>, std::decay_t<ArgTypes>...>>,
                                  bool> = true>
        explicit AsyncTask(Function&& f) : func(std::move(f)), promise(std::make_shared<std::promise<R>>()) {}

        void operator()(ArgTypes... args) {
            try {
                R ret = func(std::forward<ArgTypes>(args)...);
                promise->set_value(std::move(ret));
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
        }

        AsyncTaskFuture<R> makeFuture(AsyncTaskExecutor* owner) { return AsyncTaskFuture(promise, owner); }

      private:
        std::function<R(ArgTypes...)> func;
        std::shared_ptr<std::promise<R>> promise;
    };

    class AsyncTaskWrapper {
      public:
        explicit AsyncTaskWrapper(std::function<void(void)>&& func, void* asyncTaskPtr)
            : func(func), asyncTaskPtr(asyncTaskPtr) {}

        ~AsyncTaskWrapper() noexcept {
            if (asyncTaskPtr) {
                std::free(asyncTaskPtr);
            }
        }

        void operator()() { func(); }

      private:
        std::function<void(void)> func;
        void* asyncTaskPtr;
    };

  public:
    explicit AsyncTaskExecutor(uint32_t numOfThreads = 1);

    ~AsyncTaskExecutor();

    bool destroy();

    template<class Function, class... Args>
    [[nodiscard]] AsyncTaskFuture<std::invoke_result_t<std::decay_t<Function>, std::decay_t<Args>...>> runAsync(Function&& f,
                                                                                                                Args&&... args) {
        if (!running) {
            throw Exceptions::RuntimeException("Async Executor is destroyed");
        }
        auto taskPtr = malloc(sizeof(AsyncTask<std::invoke_result_t<std::decay_t<Function>, std::decay_t<Args>...>, Args...>));
        NES_ASSERT(taskPtr, "Cannot allocate async task");
        auto* task =
            new (taskPtr) AsyncTask<std::invoke_result_t<std::decay_t<Function>, std::decay_t<Args>...>, Args...>(std::move(f));
        auto future = task->makeFuture(this);
        std::function<void(void)> asyncCallback = [task, args...]() mutable {
            (*task)(std::forward<Args>(args)...);
        };
        {
            std::unique_lock lock(workMutex);
            asyncTaskQueue.emplace_back(std::move(asyncCallback), taskPtr);
            cv.notify_all();
        }
        return future;
    }

  private:
    void runningRoutine();

  private:
    mutable std::mutex workMutex;

    std::condition_variable cv;

    std::vector<std::shared_ptr<std::thread>> runningThreads;

    std::atomic<bool> running;

    std::vector<std::shared_ptr<std::promise<bool>>> completionPromises;

    std::deque<AsyncTaskWrapper> asyncTaskQueue;
};
using AsyncTaskExecutorPtr = std::shared_ptr<AsyncTaskExecutor>;

}// namespace NES::Runtime

#endif//NES_NES_CORE_INCLUDE_RUNTIME_ASYNCTASKEXECUTOR_HPP_
