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

#include <BinaryStoreWriter.hpp>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <utility>

#include <ErrorHandling.hpp>
#include <ReplayStoreFormat.hpp>

#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

namespace NES::StoreManager
{

BinaryStoreWriter::BinaryStoreWriter(Config cfg) : config(std::move(cfg))
{
}

BinaryStoreWriter::~BinaryStoreWriter()
{
    if (fd >= 0)
    {
        ::close(fd);
        fd = -1;
    }
}

void BinaryStoreWriter::open()
{
    constexpr int flags = O_CREAT | O_WRONLY;
    const std::string& filePath = config.filePath;
    fd = ::open(filePath.c_str(), flags, 0644);
    if (fd < 0)
    {
        throw CannotOpenSink("Could not open output file: {} (errno={}, msg={})", filePath, errno, std::strerror(errno));
    }
    struct stat st{};
    if (::fstat(fd, &st) != 0)
    {
        const int err = errno;
        ::close(fd);
        fd = -1;
        throw CannotOpenSink("fstat failed for {}: errno={}, msg={}", filePath, err, std::strerror(err));
    }
    tail.store(static_cast<uint64_t>(st.st_size), std::memory_order_relaxed);
    headerWritten.store(st.st_size > 0, std::memory_order_relaxed);
}

void BinaryStoreWriter::close()
{
    if (fd >= 0)
    {
        ::fsync(fd);
        ::close(fd);
        fd = -1;
    }
}

void BinaryStoreWriter::removeFile()
{
    close();
    const std::string& filePath = config.filePath;
    std::remove(filePath.c_str());
}

void BinaryStoreWriter::ensureHeader()
{

    bool expected = false;
    if (!headerWritten.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
    {
        return;
    }

    if (tail.load(std::memory_order_relaxed) > 0)
    {
        return;
    }

    auto buf = serializeHeader(config.schemaText);

    const uint64_t off = tail.fetch_add(buf.size(), std::memory_order_relaxed);
    if (off != 0)
    {
        return; // another writer raced
    }
    const ssize_t written = ::pwrite(fd, buf.data(), buf.size(), 0);
    if (written < 0 || static_cast<size_t>(written) != buf.size())
    {
        throw CannotOpenSink("Writing store header failed: errno={} {}", errno, std::strerror(errno));
    }
}

void BinaryStoreWriter::append(const uint8_t* data, size_t len)
{
    if (len == 0)
    {
        return;
    }
    if (fd < 0)
    {
        throw CannotOpenSink("ReplayStoreWriter not started; fd is invalid");
    }
    const uint64_t off = tail.fetch_add(len, std::memory_order_relaxed);
    const ssize_t written = ::pwrite(fd, data, len, static_cast<off_t>(off));
    if (written < 0 || static_cast<size_t>(written) != len)
    {
        throw CannotOpenSink("pwrite failed: errno={} {}", errno, std::strerror(errno));
    }
}

} // namespace NES::Replay
