#include <map>
#include <vector>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <thread>

#include <Runtime/BufferManager.hpp>

namespace iotdb {

class ThreadPoolForBufferManager {
  public:
    ThreadPoolForBufferManager() : run(), threads() {}
    ~ThreadPoolForBufferManager()
    {
        std::cout << "Destroying Thread Pool For Buffer" << std::endl;
        stop();
    }
    void start()
    {
        iotdb::BufferManager::instance();
        std::cout << "Start threads" << std::endl;

        if (run)
            return;
        run = true;

        /* spawn threads */
        auto num_threads = std::thread::hardware_concurrency();

        std::cout << "Spawning " << num_threads << " threads" << std::endl;
        for (uint64_t i = 0; i < num_threads; ++i) {
            threads.push_back(std::thread(std::bind(&ThreadPoolForBufferManager::worker_thread, this)));
        }
    }

    void worker_thread()
    {
        BufferManager& buffer_manager = BufferManager::instance();
        while (run) {

            TupleBufferPtr buf = buffer_manager.getBuffer();

            int sleep1 = std::rand() % 3 + 1;
            std::this_thread::sleep_for(std::chrono::seconds(sleep1));

            buffer_manager.releaseBuffer(buf);
            int sleep2 = std::rand() % 3 + 1;
            std::this_thread::sleep_for(std::chrono::seconds(sleep2));
        }
    }
    void stop()
    {
        if (!run)
            return;
        run = false;
        /* wake up all threads in the dispatcher,
         * so they notice the change in the run variable */
        BufferManager::instance().unblockThreads();
        /* join all threads if possible */
        for (auto& thread : threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

  private:
    bool run;
    std::vector<std::thread> threads;
};

void testSingleThread()
{
    int size = 3 * 4 * 1024;
    const int capacity = 3;
    const int num_to_release = 2;
    iotdb::BufferManager::instance();

    std::vector<TupleBufferPtr> buffers;
    std::cout << "==========Get Buffer==========" << std::endl;
    for (int i = 0; i < capacity; i++) {
        buffers.push_back(BufferManager::instance().getBuffer());
        std::cout << "buffer " << i << ": " << buffers[i]->buffer << std::endl;
    }

    std::cout << "==========Release Buffer==========" << std::endl;
    for (int i = 0; i < capacity; i++) {
        if (buffers[i]->buffer) { // make sure not nullptr
            std::cout << "try to release buffer " << buffers[i]->buffer << std::endl;
            BufferManager::instance().releaseBuffer(buffers[i]);
        }
        else {
            std::cout << "empty buffer found!!!" << std::endl;
        }
    }

    std::cout << "==========Get Buffer Again==========" << std::endl;
    for (int i = capacity - num_to_release; i < capacity; i++) {
        buffers.push_back(BufferManager::instance().getBuffer());
        std::cout << "buffer " << i << ": " << buffers[i]->buffer << std::endl;
    }

    std::cout << "==========Release Buffer==========" << std::endl;
    for (int i = 0; i < capacity; i++) {
        std::cout << "buffer " << i << ": " << buffers[i]->buffer << std::endl;
        if (buffers[i]->buffer) { // make sure not nullptr
            BufferManager::instance().releaseBuffer(buffers[i]);
        }
    }
}

void testMultiThreads()
{
    ThreadPoolForBufferManager thread_pool;
    thread_pool.start();
    int sleep = 5;
    std::cout << "Waiting " << sleep << " seconds " << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(sleep));
}
} // namespace iotdb

int main(int argc, const char* argv[])
{
    iotdb::BufferManager::instance();

    std::cout << "Test Single Thread" << std::endl;
    iotdb::testSingleThread();
    std::cout << "Test Multiple Thread" << std::endl;
    //  iotdb::testMultiThreads();

    return 0;
}
