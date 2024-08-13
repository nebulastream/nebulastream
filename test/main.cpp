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

#include <algorithm>
#include <fstream>
#include <iostream>

#include <ostream>
#include <random>
#include <thread>
#include <utility>
#include <folly/MPMCQueue.h>

static constexpr char SEPARATOR = '\n';
static constexpr size_t buffer_size = 128;

struct Buffer
{
    size_t size;
    std::vector<char> buffer = std::vector<char>(buffer_size);
};

struct Task
{
    size_t seq = 0;
    Buffer pre{{}};
    Buffer current{{}};
};

template <typename T>
T randomFrom(const T min, const T max)
{
    static std::random_device rdev;
    static std::default_random_engine re(rdev());
    using dist_type = std::conditional_t<std::is_floating_point_v<T>, std::uniform_real_distribution<T>, std::uniform_int_distribution<T>>;
    dist_type uni(min, max);
    return static_cast<T>(uni(re));
}


std::optional<Buffer> generate_chunk(std::ifstream& f)
{
    if (f.eof())
    {
        return std::nullopt;
    }

    Buffer buffer;
    f.read(buffer.buffer.data(), buffer.buffer.size());
    buffer.size = f.gcount();
    return buffer;
}

using Queue = folly::MPMCQueue<Task>;

class WorkerThread
{
public:
    explicit WorkerThread(Queue& queue) : queue(queue) { }
    void operator()(const std::stop_token& stok)
    {
        while (!stok.stop_requested() || !queue.isEmpty())
        {
            Task task;
            if (!queue.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(500), task))
            {
                continue;
            }

            auto& predecessorBuffer = task.pre.buffer;
            auto& currentBuffer = task.current.buffer;

            std::vector<char> newBuffer;

            auto lastNewlineInPredecessor = std::find(predecessorBuffer.rbegin(), predecessorBuffer.rend(), '\n');

            auto lastNewlineInCurrent = std::find(currentBuffer.rbegin(), currentBuffer.rend(), '\n');

            auto startPosInPredecessor = predecessorBuffer.end();
            if (lastNewlineInPredecessor != predecessorBuffer.rend())
            {
                startPosInPredecessor = lastNewlineInPredecessor.base();
            }

            auto endPosInCurrent = currentBuffer.end();
            if (lastNewlineInCurrent != currentBuffer.rend())
            {
                endPosInCurrent = lastNewlineInCurrent.base();
            }

            // Copy the required parts into the new buffer
            newBuffer.insert(newBuffer.end(), startPosInPredecessor, predecessorBuffer.end());
            newBuffer.insert(newBuffer.end(), currentBuffer.begin(), endPosInCurrent);

            newBuffer.push_back('\0');

            std::cout << newBuffer.data() << '\n';
        }
    }

private:
    Queue& queue;
};

int main()
{
    Queue queue(20);
    std::jthread worker{WorkerThread(queue)};
    std::jthread worker1{WorkerThread(queue)};
    std::ifstream f("/home/ls/dima/nebulastream-public/test/testfile2.txt");
    if (!f)
    {
        std::cerr << strerror(errno) << '\n';
    }
    assert(f && "Failed to open file");

    auto pre = Buffer{{}};
    for (size_t i = 0; i < 200; i++)
    {
        auto current = generate_chunk(f);
        if (!current)
        {
            break;
        }

        Task task{i, std::move(pre), *current};
        std::swap(pre, *current);
        queue.write(std::move(task));
    }
}