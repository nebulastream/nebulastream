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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_ASYNCTASKEXECUTOR_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_ASYNCTASKEXECUTOR_HPP_

#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/Logger/Logger.hpp>
#include <condition_variable>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace NES::Runtime {

/**
 * @brief This is a multi-threaded task executor that execute asynchronously tasks. Submitted task are paired to a future value
 * that can be retrieved when the async task is executed.
 */
class AsyncTaskExecutor {
  public:
    /**
     * @brief This is a future that the async executor returns, which can be retrieved using `.wait()`
     * @tparam R the return type
     */
    template<class R>
    class AsyncTaskFuture {
      public:
        /**
         * @brief Creates an AsyncTaskFuture from a promise and a reference to the executor
         * @param promise the promise that the executor will fulfill
         * @param owner the executor
         */
        explicit AsyncTaskFuture(std::shared_ptr<std::promise<R>> promise, AsyncTaskExecutor& owner)
            : promise(std::move(promise)), owner(owner) {
            future = this->promise->get_future();
        }

        AsyncTaskFuture(const AsyncTaskFuture& that) : promise(that.promise), future(that.future), owner(that.owner) {}

        AsyncTaskFuture& operator=(const AsyncTaskFuture& that) {
            promise = that.promise;
            future = that.future;
            owner = that.owner;
            return *this;
        }

        /**
         * @brief This call blocks until the promise is fulfilled and returns the produced value
         * @return
         */
        [[nodiscard]] inline R wait() { return future.get(); }

        /**
         * @brief This method creates a new (inner) AsyncTaskFuture from an already existing (*this) AsyncTaskFuture.
         * When the *this AsyncTaskFuture is fulfilled, its internal value is passed to f and the return value is set as internal
         * value of the inner AsyncTaskFuture.
         * @tparam Function the function type to execute on the promise value of the *this AsyncTaskFuture
         * @param f the function to execute on the promise value of the *this AsyncTaskFuture
         * @return a new inner AsyncTaskFuture
         */
        template<class Function>
        [[nodiscard]] AsyncTaskFuture<std::invoke_result_t<std::decay_t<Function>, std::decay_t<R>>> thenAsync(Function&& f) {
            return owner.runAsync([this, f = std::move(f)]() {
                return f(future.get());
            });
        }

        /**
         * @brief operator! returns true if the AsyncTaskFuture is invalid
         * @return true if the AsyncTaskFuture is invalid
         */
        [[nodiscard]] inline bool operator!() const { return promise == nullptr; }

        /**
         * @brief operator bool() returns true if the AsyncTaskFuture is valid
         * @return true if the AsyncTaskFuture is valid
         */
        [[nodiscard]] inline operator bool() const { return promise != nullptr; }

      private:
        std::shared_ptr<std::promise<R>> promise;// we need this variable to keep the promise machinery alive
        std::shared_future<R> future;
        AsyncTaskExecutor& owner;
    };

  private:
    /**
     * @brief This is the internal task that the executor runs internally.
     * @tparam R the return type of the AsyncTask
     * @tparam ArgTypes the arguments types to pass to the AsyncTask functor
     */
    template<class R, class... ArgTypes>
    class AsyncTask {
      public:
        /**
         * @brief Creates an AsyncTask from a function f
         * @tparam Function the function type
         * @param f the function to execute on the async executor
         */
        template<class Function,
                 std::enable_if_t<std::is_same_v<R, std::invoke_result_t<std::decay_t<Function>, std::decay_t<ArgTypes>...>>,
                                  bool> = true>
        explicit AsyncTask(Function&& f) : func(std::move(f)), promise(std::make_shared<std::promise<R>>()) {}

        /**
         * @brief Executes the function on a variadic set of arguments
         * @param args the arguments
         */
        inline void operator()(ArgTypes... args) {
            try {
                R ret = func(std::forward<ArgTypes>(args)...);
                promise->set_value(std::move(ret));
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
        }

        /**
         * @brief Creates an AsyncTaskFuture out of the current task. The AsyncTaskFuture is to be used to retrieve the AsyncTask
         * produced value
         * @param owner the owning executor
         * @return the AsyncTaskFuture to wait on
         */
        inline AsyncTaskFuture<R> makeFuture(AsyncTaskExecutor& owner) { return AsyncTaskFuture(promise, owner); }

      private:
        std::function<R(ArgTypes...)> func;
        std::shared_ptr<std::promise<R>> promise;
    };

    /**
     * @brief a type-less wrapper of the AsyncTask. Necessary to store this in a simple container like an `std::dequeue`
     */
    class AsyncTaskWrapper {
      public:
        /**
         * @brief Creates a new AsyncTaskWrapper from a function that wraps an AsyncTask
         * and a void pointer to the asyncTaskPtr (for garbage collection)
         * @param func the function to execute
         * @param asyncTaskPtr
         */
        explicit AsyncTaskWrapper(std::function<void(void)>&& func, std::function<void(void)>&& releaseCallback)
            : func(std::move(func)), gc(std::move(releaseCallback)) {
            NES_VERIFY(!!this->func, "Invalid callback");
            NES_VERIFY(!!this->gc, "Invalid gc callback");
        }

        ~AsyncTaskWrapper() noexcept = default;

        /// executes the inner AsyncTask
        inline void operator()() {
            func();
            gc();// deallocates the internal AsyncTask for garbage collection
        }

      private:
        std::function<void(void)> func;
        std::function<void(void)> gc;
    };

  public:
    /**
     * @brief Creates an AsyncTaskExecutor using `numOfThreads` threads
     * @param numOfThreads the number of threads to use for the executor
     */
    explicit AsyncTaskExecutor(const HardwareManagerPtr& hardwareMananger, uint32_t numOfThreads = 1);

    /// destructor to clean up inner resources
    ~AsyncTaskExecutor();

    /**
     * @brief Method to clean up the internal resources (called by the destructor).
     * Only the first invocation cleans inner resources. Subsequent calls won't produced any effect.
     * @return
     */
    bool destroy();

    /**
     * @brief Submits an async task to the executor to be executed in the future
     * @param f function to execute
     * @param args its arguments
     * @return a future to the completable future
     */
    template<class Function, class... Args>
    [[nodiscard]] AsyncTaskFuture<std::invoke_result_t<std::decay_t<Function>, std::decay_t<Args>...>> runAsync(Function&& f,
                                                                                                                Args&&... args) {
        if (!running) {
            throw Exceptions::RuntimeException("Async Executor is destroyed");
        }
        // allocates a task on the heap and captures all arguments and types in the AsyncTask
        // next, everything is captured in the AsyncTaskWrapper, which has no types.
        constexpr auto taskSize = sizeof(AsyncTask<std::invoke_result_t<std::decay_t<Function>, std::decay_t<Args>...>, Args...>);
        auto* taskPtr = allocateAsyncTask(taskSize);
        NES_ASSERT(taskPtr, "Cannot allocate async task");
        auto* task =
            new (taskPtr) AsyncTask<std::invoke_result_t<std::decay_t<Function>, std::decay_t<Args>...>, Args...>(std::move(f));
        auto future = task->makeFuture(*this);
        std::function<void(void)> asyncCallback = [task, args...]() mutable {
            (*task)(std::forward<Args>(args)...);
        };
        std::function<void(void)> releaseCallback = [this, taskPtr]() {
            deallocateAsyncTask(taskPtr, taskSize);
        };
        NES_ASSERT(!!asyncCallback, "Cannot allocate async task");
        NES_ASSERT(!!releaseCallback, "Cannot allocate async task");
        {
            std::unique_lock lock(workMutex);
            asyncTaskQueue.emplace_back(std::move(asyncCallback), std::move(releaseCallback));
            cv.notify_all();
        }
        return future;
    }

  private:
    /// the inner thread routine that executes async tasks
    void runningRoutine();

    void* allocateAsyncTask(size_t taskSize);

    void deallocateAsyncTask(void* task, size_t size);

  private:
    mutable std::mutex workMutex;

    std::condition_variable cv;

    std::vector<std::shared_ptr<std::thread>> runningThreads;

    std::atomic<bool> running;

    std::vector<std::shared_ptr<std::promise<bool>>> completionPromises;

    std::deque<AsyncTaskWrapper> asyncTaskQueue;

    HardwareManagerPtr hardwareManager;
};
using AsyncTaskExecutorPtr = std::shared_ptr<AsyncTaskExecutor>;

}// namespace NES::Runtime

#endif  // NES_RUNTIME_INCLUDE_RUNTIME_ASYNCTASKEXECUTOR_HPP_
