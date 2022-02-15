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

#include <Util/FileMutex.hpp>
#include <Util/Logger.hpp>
#include <errno.h>
#include <cstring>
#include <unistd.h>
#ifdef __linux__
#include <sys/fcntl.h>
#elif defined(__APPLE__)
#include <fcntl.h>
#else
#error "Unsupported platform"
#endif

namespace NES::Util {

FileMutex::FileMutex(const std::string filePath) : fileName(filePath) {
    fd = open(filePath.c_str(), O_RDWR | O_CREAT, S_IRWXU);
    if (fd == -1 && errno == EEXIST) {
        fd = open(filePath.c_str(), O_RDWR);
    }
    NES_ASSERT2_FMT(fd != -1, "Invalid file " << filePath << " " << strerror(errno));
}

FileMutex::~FileMutex() {
    close(fd);
    unlink(fileName.c_str());
}

void FileMutex::lock() {
    struct flock lock;
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    NES_ASSERT(-1 != ::fcntl(fd, F_SETLKW, &lock), "Cannot acquire lock");
}

bool FileMutex::try_lock() {
    bool acquired = false;
    struct flock lock;
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    int ret = fcntl(fd, F_SETLK, &lock);
    if (ret == -1) {
        return (errno == EAGAIN || errno == EACCES) ? (acquired = false, true) : false;
    }
    return (acquired = true);
}

void FileMutex::unlock() {
    struct flock lock;
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    NES_ASSERT(-1 != fcntl(fd, F_SETLK, &lock), "Cannot acquire lock");
}

}// namespace NES::Util