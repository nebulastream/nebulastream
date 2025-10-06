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
#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace NES::Util {

class MappedFile {
  public:
    explicit MappedFile(const std::string& path) {
        fd_ = ::open(path.c_str(), O_RDONLY);
        if (fd_ < 0) {
            throw std::runtime_error("Failed to open file: " + path);
        }
        struct stat st {};
        if (::fstat(fd_, &st) != 0) {
            ::close(fd_);
            throw std::runtime_error("Failed to fstat file: " + path);
        }
        size_ = static_cast<size_t>(st.st_size);
        if (size_ == 0) {
            // Map empty as nullptr
            data_ = nullptr;
            return;
        }
        void* addr = ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
        if (addr == MAP_FAILED) {
            ::close(fd_);
            throw std::runtime_error("Failed to mmap file: " + path);
        }
        data_ = static_cast<const uint8_t*>(addr);
        path_ = path;
    }

    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;

    MappedFile(MappedFile&& other) noexcept { moveFrom(std::move(other)); }
    MappedFile& operator=(MappedFile&& other) noexcept {
        if (this != &other) {
            cleanup();
            moveFrom(std::move(other));
        }
        return *this;
    }

    ~MappedFile() { cleanup(); }

    const uint8_t* data() const { return data_; }
    size_t size() const { return size_; }
    const std::string& path() const { return path_; }

  private:
    void moveFrom(MappedFile&& other) noexcept {
        fd_ = other.fd_;
        size_ = other.size_;
        data_ = other.data_;
        path_ = std::move(other.path_);
        other.fd_ = -1;
        other.size_ = 0;
        other.data_ = nullptr;
    }

    void cleanup() {
        if (data_) {
            ::munmap(const_cast<uint8_t*>(data_), size_);
            data_ = nullptr;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    int fd_{-1};
    size_t size_{0};
    const uint8_t* data_{nullptr};
    std::string path_;
};

}


