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

#ifndef NES_COMMON_TESTS_UTIL_INCLUDE_DETAIL_SHAREDMEMORYFIXEDVECTOR_HPP_
#define NES_COMMON_TESTS_UTIL_INCLUDE_DETAIL_SHAREDMEMORYFIXEDVECTOR_HPP_

#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
namespace NES::Testing::detail {

template<typename T>
class SharedMemoryFixedVector {
    struct Metadata {
        std::atomic<uint32_t> refCnt;
        std::atomic<uint64_t> currentIndex;
    };

  public:
    explicit SharedMemoryFixedVector(const std::string& name, size_t capacity)
        : name(name), mmapSize(sizeof(T) * capacity + sizeof(Metadata)), capacity(capacity), created(false) {}

    void open() {
        while (true) {
            shmemFd = shm_open(name.c_str(), O_RDWR | O_CREAT | O_EXCL, S_IRWXU);
            if (shmemFd >= 0) {
                fchmod(shmemFd, S_IRWXU);
                created = true;
            } else if (errno == EEXIST) {
                shmemFd = shm_open(name.c_str(), O_RDWR, S_IRWXU);
                if (shmemFd < 0 && errno == ENOENT) {
                    continue;
                }
            }
            break;
        }
        if (shmemFd == -1) {
            std::cerr << "cannot create shared area: " << strerror(errno) << std::endl;
        }
        NES_ASSERT2_FMT(shmemFd != -1, "cannot create shared area: " << strerror(errno));
        if (created) {
            auto ret = ftruncate(shmemFd, mmapSize) == 0;
            if (!ret) {
                std::cerr << "cannot ftruncate shared area: " << strerror(errno) << std::endl;
            }
            NES_ASSERT(ret, "cannot create shared area");
        } else {
            std::string filePath = std::filesystem::temp_directory_path() / "nes.tests.begin";
            while (!std::filesystem::exists(filePath)) {
                NES_DEBUG("File {} does not exists", filePath);
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
        mem = reinterpret_cast<uint8_t*>(mmap(nullptr, mmapSize, PROT_READ | PROT_WRITE, MAP_SHARED, shmemFd, 0));
        if (mem == MAP_FAILED) {
            std::cerr << "cannot create shared area: " << strerror(errno) << std::endl;
        }
        NES_ASSERT(mem != MAP_FAILED, "cannot create shared area");
        metadata = reinterpret_cast<Metadata*>(mem);
        vectorData = reinterpret_cast<T*>(mem + sizeof(Metadata));
        if (created) {
            metadata->refCnt.store(1);
            metadata->currentIndex.store(0);
            auto filePath = std::filesystem::temp_directory_path() / "nes.tests.begin";
            std::ofstream file;
            file.open(filePath);
            NES_ASSERT2_FMT(file.good(), "cannot open file " << filePath);
            file.close();
        } else {
            metadata->refCnt.fetch_add(1);
        }
    }

    ~SharedMemoryFixedVector() {
        bool doCleanup = metadata->refCnt.fetch_sub(1) == 1;
        NES_ASSERT(0 == munmap(mem, mmapSize), "Cannot munmap");
        NES_ASSERT(0 == close(shmemFd), "Cannot close");
        if (doCleanup) {
            if (0 == shm_unlink(name.c_str())) {
                // no process has the shared memory segment open
                std::filesystem::remove_all(std::filesystem::temp_directory_path() / "nes.tests.begin");
            }
        }
    }

    T* data() { return vectorData; }

    T& operator[](size_t index) { return vectorData[index]; }

    uint64_t getNextIndex() { return metadata->currentIndex.fetch_add(1) % capacity; }

    [[nodiscard]] bool isCreated() const { return created; }

  private:
    std::string name;
    int shmemFd;
    size_t mmapSize;
    size_t capacity;
    uint8_t* mem;
    T* vectorData;
    Metadata* metadata;
    bool created;
};
}// namespace NES::Testing::detail
#endif // NES_COMMON_TESTS_UTIL_INCLUDE_DETAIL_SHAREDMEMORYFIXEDVECTOR_HPP_
