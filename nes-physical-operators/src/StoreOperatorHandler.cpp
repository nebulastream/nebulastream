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

#include <StoreOperatorHandler.hpp>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>
#include <utility>

#include <Runtime/QueryTerminationType.hpp>
#include <ErrorHandling.hpp>
#include "Runtime/Execution/OperatorHandler.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace NES
{

namespace
{
constexpr char MAGIC[8] = {'N', 'E', 'S', 'S', 'T', 'O', 'R', 'E'};
constexpr uint32_t VERSION = 1;
constexpr uint8_t ENDIANNESS_LE = 1;
}

StoreOperatorHandler::StoreOperatorHandler(Config cfg) : config(std::move(cfg))
{
}

void StoreOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
    openFile();
    writeHeaderIfNeeded();
}

void StoreOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
    if (fd >= 0)
    {
        ::fsync(fd);
        ::close(fd);
        fd = -1;
    }
}

void StoreOperatorHandler::ensureHeader(PipelineExecutionContext&)
{
    writeHeaderIfNeeded();
}

void StoreOperatorHandler::append(const uint8_t* data, size_t len)
{
    if (len == 0)
    {
        return;
    }
    if (fd < 0)
    {
        throw CannotOpenSink("StoreOperatorHandler not started; fd is invalid");
    }
    const uint64_t off = tail.fetch_add(len, std::memory_order_relaxed);
    const ssize_t written = ::pwrite(fd, data, len, static_cast<off_t>(off));
    if (written < 0 || static_cast<size_t>(written) != len)
    {
        throw CannotOpenSink("pwrite failed: errno={} {}", errno, std::strerror(errno));
    }
}

void StoreOperatorHandler::openFile()
{
    constexpr int flags = O_CREAT | O_WRONLY;
    fd = ::open(config.filePath.c_str(), flags, 0644);
    if (fd < 0)
    {
        throw CannotOpenSink("Could not open output file: {} (errno={}, msg={})", config.filePath, errno, std::strerror(errno));
    }
    struct stat st{};
    if (::fstat(fd, &st) != 0)
    {
        const int err = errno;
        ::close(fd);
        fd = -1;
        throw CannotOpenSink("fstat failed for {}: errno={}, msg={}", config.filePath, err, std::strerror(err));
    }
    tail.store(static_cast<uint64_t>(st.st_size), std::memory_order_relaxed);
    headerWritten.store(st.st_size > 0, std::memory_order_relaxed);
}

void StoreOperatorHandler::writeHeaderIfNeeded()
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

    const uint64_t fingerprint = fnv1a64(config.schemaText.c_str(), config.schemaText.size());
    const auto schemaLen = static_cast<uint32_t>(config.schemaText.size());

    const size_t headerSize
        = sizeof(MAGIC) + sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t) + schemaLen;
    std::string buf;
    buf.resize(headerSize);
    size_t off = 0;
    std::memcpy(buf.data() + off, MAGIC, sizeof(MAGIC));
    off += sizeof(MAGIC);
    std::memcpy(buf.data() + off, &VERSION, sizeof(uint32_t));
    off += sizeof(uint32_t);
    std::memcpy(buf.data() + off, &ENDIANNESS_LE, sizeof(uint8_t));
    off += sizeof(uint8_t);
    constexpr uint32_t flags = 0;
    std::memcpy(buf.data() + off, &flags, sizeof(uint32_t));
    off += sizeof(uint32_t);
    std::memcpy(buf.data() + off, &fingerprint, sizeof(uint64_t));
    off += sizeof(uint64_t);
    std::memcpy(buf.data() + off, &schemaLen, sizeof(uint32_t));
    off += sizeof(uint32_t);
    std::memcpy(buf.data() + off, config.schemaText.data(), schemaLen);
    off += schemaLen;

    const uint64_t off0 = tail.fetch_add(headerSize, std::memory_order_relaxed);
    if (off0 != 0)
    {
        return;
    }
    const ssize_t written = ::pwrite(fd, buf.data(), buf.size(), 0);
    if (written < 0 || static_cast<size_t>(written) != buf.size())
    {
        throw CannotOpenSink("Writing store header failed: errno={} {}", errno, std::strerror(errno));
    }
}

uint64_t StoreOperatorHandler::fnv1a64(const char* data, size_t len)
{
    uint64_t hash = FNVOffsetBasis;
    for (size_t i = 0; i < len; ++i)
    {
        hash ^= static_cast<uint8_t>(data[i]);
        hash *= FNVPrime;
    }
    return hash;
}

}

