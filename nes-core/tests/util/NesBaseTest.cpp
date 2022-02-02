/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include "NesBaseTest.hpp"
#include "Util/UtilityFunctions.hpp"
#include <Util/FileMutex.hpp>
#include <Util/Logger.hpp>
#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <pthread.h>
#include <random>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace NES::Testing {

namespace detail {

template<typename T>
class ShmFixedVector {
    struct Metadata {
        std::atomic<uint32_t> refCnt;
        std::atomic<uint64_t> currentIndex;
    };

  public:
    explicit ShmFixedVector(const std::string& name, size_t capacity)
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
            auto ret = ftruncate(shmemFd, mmapSize) != -1;
            if (ret) {
                std::cerr << "cannot create shared area: " << strerror(errno) << std::endl;
            }
            NES_ASSERT(ret, "cannot create shared area");
        } else {
            while (!std::filesystem::exists("/tmp/nes.tests.begin")) {
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
            std::filesystem::create_directories("/tmp/nes.tests.begin");
        } else {
            metadata->refCnt.fetch_add(1);
        }
    }

    ~ShmFixedVector() {
        if (metadata->refCnt.fetch_sub(1) == 1) {
            NES_ASSERT(0 == munmap(mem, mmapSize), "Cannot munmap");
            NES_ASSERT(0 == close(shmemFd), "Cannot close");
            NES_ASSERT(0 == shm_unlink(name.c_str()), "Cannot shm_unlink");
            std::filesystem::remove_all("/tmp/nes.tests.begin");
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

namespace uuid {
static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_int_distribution<> dis(0, 15);
static std::uniform_int_distribution<> dis2(8, 11);

std::string generateUUID() {
    std::stringstream ss;
    int i;
    ss << std::hex;
    for (i = 0; i < 8; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (i = 0; i < 4; i++) {
        ss << dis(gen);
    }
    ss << "-4";
    for (i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    ss << dis2(gen);
    for (i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (i = 0; i < 12; i++) {
        ss << dis(gen);
    };
    return ss.str();
}
}// namespace uuid
}// namespace detail
class NesPortDispatcher {
  private:
    static constexpr uint32_t CHECKSUM = 0x30011991;
    struct PortHolder {
        std::atomic<uint16_t> port;
        std::atomic<bool> free;
        std::atomic<uint32_t> checksum;
    };

  public:
    explicit NesPortDispatcher(uint16_t startPort, uint32_t numberOfPorts)
        : mutex(std::filesystem::temp_directory_path() / "nes.lock"), data("/nes.port.pool", numberOfPorts) {
        std::unique_lock<Util::FileMutex> lock(mutex, std::defer_lock);
        if (lock.try_lock()) {
            data.open();
            if (data.isCreated()) {
                for (uint16_t current = startPort, i = 0; i < numberOfPorts; ++i, ++current) {
                    data[i].port = current;
                    data[i].free = true;
                    data[i].checksum = CHECKSUM;
                }
            }
        } else {
            lock.lock();
            data.open();
        }
    }

    BorrowedPortPtr getNextPort() {
        while (true) {
            auto nextIndex = data.getNextIndex();
            auto expected = true;
            if (data[nextIndex].checksum == CHECKSUM) {
                std::cerr << "invalid checksum " << data[nextIndex].checksum << std::endl;
            }
            NES_ASSERT2_FMT(data[nextIndex].checksum == CHECKSUM, "invalid checksum");
            if (data[nextIndex].free.compare_exchange_strong(expected, false)) {
                return std::make_shared<BorrowedPort>(data[nextIndex].port, nextIndex, this);
            }
            usleep(100 * 1000);// 100ms
        }
        std::cerr << "Cannot find free port" << std::endl;
        NES_ASSERT(false, "Cannot find free port");
        return nullptr;
    }

    void recyclePort(uint32_t portIndex) { data[portIndex].free.store(true); }

  private:
    Util::FileMutex mutex;
    detail::ShmFixedVector<PortHolder> data;
    //        uint32_t numberOfPorts;
};

static NesPortDispatcher portDispatcher(8081, 10000);

NESBaseTest::NESBaseTest() : testResourcePath(std::filesystem::current_path() / detail::uuid::generateUUID()) {
    if (!std::filesystem::exists(testResourcePath)) {
        std::filesystem::create_directories(testResourcePath);
    } else {
        std::filesystem::remove_all(testResourcePath);
        std::filesystem::create_directories(testResourcePath);
    }
}

void NESBaseTest::SetUp() {
    restPort = portDispatcher.getNextPort();
    rpcCoordinatorPort = portDispatcher.getNextPort();
}

BorrowedPortPtr NESBaseTest::getAvailablePort() { return portDispatcher.getNextPort(); }

std::filesystem::path NESBaseTest::getTestResourceFolder() const { return testResourcePath; }

void NESBaseTest::TearDown() {
    restPort.reset();
    rpcCoordinatorPort.reset();
    std::filesystem::remove_all(testResourcePath);
}

BorrowedPort::~BorrowedPort() noexcept { parent->recyclePort(portIndex); }

}// namespace NES::Testing