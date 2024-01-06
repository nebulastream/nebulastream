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

#include <NesBaseTest.hpp>
#include <Util/FileMutex.hpp>
#include <Util/Logger/Logger.hpp>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <random>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#if defined(__linux__)
#include <pwd.h>
#endif

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
            auto ret = ftruncate(shmemFd, mmapSize) == 0;
            if (!ret) {
                std::cerr << "cannot ftruncate shared area: " << strerror(errno) << std::endl;
            }
            NES_ASSERT(ret, "cannot create shared area");
        } else {
            while (!std::filesystem::exists(std::filesystem::temp_directory_path() / "nes.tests.begin")) {
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

    ~ShmFixedVector() {
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

TestWaitingHelper::TestWaitingHelper() { testCompletion = std::make_shared<std::promise<bool>>(); }

void TestWaitingHelper::failTest() {
    auto expected = false;
    if (testCompletionSet.compare_exchange_strong(expected, true)) {
        testCompletion->set_value(false);
//        waitThread->join();
//        waitThread.reset();
    }
}

void TestWaitingHelper::completeTest() {
    auto expected = false;
    if (testCompletionSet.compare_exchange_strong(expected, true)) {
        testCompletion->set_value(true);
//        waitThread->join();
//        waitThread.reset();
    }
}

void TestWaitingHelper::startWaitingThread(std::string) {
    auto self = this;
//    waitThread = std::make_unique<std::thread>([this, testName = std::move(testName)]() mutable {
//        auto future = testCompletion->get_future();
//        switch (future.wait_for(std::chrono::minutes(WAIT_TIME_SETUP))) {
//            case std::future_status::ready: {
//                try {
//                    auto res = future.get();
//                    ASSERT_TRUE(res);
//                    if (!res) {
//                        NES_FATAL_ERROR2("Got error in test [{}]", testName);
//                        std::exit(-127);
//                    }
//                } catch (std::exception const& exception) {
//                    NES_FATAL_ERROR2("Got exception in test [{}]: {}", testName, exception.what());
//                    FAIL();
//                    std::exit(-1);
//                }
//                break;
//            }
//            case std::future_status::timeout:
//            case std::future_status::deferred: {
//                NES_ERROR2("Cannot terminate test [{}] within deadline", testName);
//                FAIL();
//                std::exit(-127);
//                break;
//            }
//        }
//    });
}

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

// trim from start (in place)
static inline void ltrim(std::string& s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                return !std::isspace(ch);
            }));
}

// trim from end (in place)
static inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(),
                         s.rend(),
                         [](unsigned char ch) {
                             return !std::isspace(ch);
                         })
                .base(),
            s.end());
}

static inline std::string trim(std::string s) {
    std::string str(s);
    rtrim(str);
    ltrim(str);
    return str;
}

static inline std::string getUsername() {
#if defined(__linux__)
    uid_t uid = geteuid();
    struct passwd* pw = getpwuid(uid);
    if (pw) {
        return std::string(pw->pw_name);
    }
#endif
    return getenv("USER");
}

static inline auto getMutexPath() {
    using namespace std::string_literals;
    auto tmpPath = std::filesystem::temp_directory_path();
    auto fileName = detail::trim(getUsername()) + ".nes.lock"s;
    return tmpPath / fileName;
}

static inline auto getPortPoolName() {
    using namespace std::string_literals;
    auto username = detail::trim(getUsername());
    return "/"s + username + ".nes.port.pool"s;
}

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
        : mutex(detail::getMutexPath()), data(detail::getPortPoolName(), numberOfPorts) {
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

    ~NesPortDispatcher() { tearDown(); }

    void tearDown() {
        NES_ASSERT2_FMT(numOfBorrowedPorts == 0,
                        "There are " << numOfBorrowedPorts << " leaked ports - please check your test logic");
    }

    BorrowedPortPtr getNextPort() {
        while (true) {
            auto nextIndex = data.getNextIndex();
            auto expected = true;
            if (data[nextIndex].checksum != CHECKSUM) {
                std::cerr << "invalid checksum " << data[nextIndex].checksum << "!=" << CHECKSUM << std::endl;
            }
            NES_ASSERT2_FMT(data[nextIndex].checksum == CHECKSUM, "invalid checksum");
            if (data[nextIndex].free.compare_exchange_strong(expected, false)) {
                ++numOfBorrowedPorts;
                return std::make_shared<BorrowedPort>(data[nextIndex].port, nextIndex, this);
            }
            usleep(100 * 1000);// 100ms
        }
        std::cerr << "Cannot find free port" << std::endl;
        NES_ASSERT(false, "Cannot find free port");
        return nullptr;
    }

    void recyclePort(uint32_t portIndex) {
        auto expected = false;
        if (data[portIndex].free.compare_exchange_strong(expected, true)) {
            --numOfBorrowedPorts;
        } else {
            NES_ASSERT2_FMT(false, "Port " << data[portIndex].port << " recycled twice");
        }
    }

  private:
    Util::FileMutex mutex;
    detail::ShmFixedVector<PortHolder> data;
    std::atomic<size_t> numOfBorrowedPorts{0};
    //        uint32_t numberOfPorts;
};

static NesPortDispatcher portDispatcher(8000, 10000);

NESBaseTest::NESBaseTest() : testResourcePath(std::filesystem::current_path() / detail::uuid::generateUUID()) {}

void NESBaseTest::SetUp() {
    auto expected = false;
    if (setUpCalled.compare_exchange_strong(expected, true)) {
        Base::SetUp();
        if (!std::filesystem::exists(testResourcePath)) {
            std::filesystem::create_directories(testResourcePath);
        } else {
            std::filesystem::remove_all(testResourcePath);
            std::filesystem::create_directories(testResourcePath);
        }
        restPort = portDispatcher.getNextPort();
        rpcCoordinatorPort = portDispatcher.getNextPort();
    } else {
        NES_ERROR2("SetUp called twice in {}", typeid(*this).name());
    }
}

BorrowedPortPtr NESBaseTest::getAvailablePort() { return portDispatcher.getNextPort(); }

std::filesystem::path NESBaseTest::getTestResourceFolder() const { return testResourcePath; }

NESBaseTest::~NESBaseTest() {
    NES_ASSERT2_FMT(setUpCalled, "SetUp not called for test " << typeid(*this).name());
    NES_ASSERT2_FMT(tearDownCalled, "TearDown not called for test " << typeid(*this).name());
}

void NESBaseTest::TearDown() {
    auto expected = false;
    if (tearDownCalled.compare_exchange_strong(expected, true)) {
        restPort.reset();
        rpcCoordinatorPort.reset();
        std::filesystem::remove_all(testResourcePath);
        Base::TearDown();
        completeTest();
        //        portDispatcher.tearDown();
    } else {
        NES_ERROR2("TearDown called twice in {}", typeid(*this).name());
    }
}

void NESBaseTest::onFatalError(int signalNumber, std::string callstack) {
    NES_ERROR2("onFatalError: signal [{}] error [{}] callstack: {}", signalNumber, strerror(errno), callstack);
    failTest();
}

void NESBaseTest::onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) {
    NES_ERROR("onFatalException: exception=[" << exception->what() << "] callstack=\n" << callstack);
    failTest();
}

BorrowedPort::~BorrowedPort() noexcept { parent->recyclePort(portIndex); }

}// namespace NES::Testing