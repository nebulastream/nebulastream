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

#include <Execution/Operators/SliceStore/FileDescriptor/FileDescriptors.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace NES::Runtime::Execution
{

FileWriter::FileWriter(
    const std::string& filePath,
    const std::function<char*()>& allocate,
    const std::function<void(char*)>& deallocate,
    const size_t bufferSize)
    : workGuard(boost::asio::make_work_guard(io))
    , file(io)
    , keyFile(io)
    , writeBuffer(allocate())
    , writeKeyBuffer(nullptr)
    , writeBufferPos(0)
    , writeKeyBufferPos(0)
    , bufferSize(bufferSize)
    , allocate(allocate)
    , deallocate(deallocate)
{
    const auto fd = open((filePath + ".dat").c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0666);
    const auto keyFd = open((filePath + "_key.dat").c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (fd < 0 || keyFd < 0)
    {
        throw std::runtime_error("Failed to open file or key file for writing");
    }

    file.assign(fd);
    keyFile.assign(keyFd);

    ioThread = std::thread([this] { runContext(); });
}

FileWriter::~FileWriter()
{
    /// Buffer needs to be flushed manually before calling the destructor
    // TODO add synchronous flush
    workGuard.reset();
    io.stop();
    if (ioThread.joinable())
    {
        ioThread.join();
    }

    deallocateBuffers();
}

boost::asio::awaitable<void> FileWriter::write(const void* data, const size_t size)
{
    if (writeBuffer == nullptr)
    {
        writeBuffer = allocate();
    }

    co_await bufferedWrite(file, writeBuffer, writeBufferPos, data, size);
    co_return;
}

boost::asio::awaitable<void> FileWriter::writeKey(const void* data, const size_t size)
{
    if (writeKeyBuffer == nullptr)
    {
        writeKeyBuffer = allocate();
    }

    co_await bufferedWrite(keyFile, writeKeyBuffer, writeKeyBufferPos, data, size);
    co_return;
}

boost::asio::awaitable<void> FileWriter::flush()
{
    if (bufferSize == 0)
    {
        co_return;
    }

    if (writeBufferPos > 0)
    {
        co_await flushStream(file, writeBuffer, writeBufferPos);
        writeBufferPos = 0;
    }
    if (writeKeyBufferPos > 0)
    {
        co_await flushStream(keyFile, writeKeyBuffer, writeKeyBufferPos);
        writeKeyBufferPos = 0;
    }
    co_return;
}

void FileWriter::deallocateBuffers()
{
    deallocate(writeBuffer);
    writeBuffer = nullptr;
    deallocate(writeKeyBuffer);
    writeKeyBuffer = nullptr;
}

boost::asio::awaitable<void>
FileWriter::bufferedWrite(boost::asio::posix::stream_descriptor& stream, char* buf, size_t& bufferPos, const void* data, size_t size) const
{
    const auto* dataPtr = static_cast<const char*>(data);
    if (bufferSize == 0)
    {
        co_await flushStream(stream, dataPtr, size);
        co_return;
    }

    while (size > 0)
    {
        const auto copySize = std::min(size, bufferSize - bufferPos);
        std::memcpy(buf + bufferPos, dataPtr, copySize);
        bufferPos += copySize;
        dataPtr += copySize;
        size -= copySize;

        if (bufferPos == bufferSize)
        {
            co_await flushStream(stream, buf, bufferPos);
            bufferPos = 0;
        }
    }
    co_return;
}

boost::asio::awaitable<void> FileWriter::flushStream(boost::asio::posix::stream_descriptor& stream, const char* buffer, const size_t size)
{
    size_t written = 0;
    while (written < size)
    {
        const size_t numBytes
            = co_await stream.async_write_some(boost::asio::buffer(buffer + written, size - written), boost::asio::use_awaitable);
        written += numBytes;
    }
    co_return;
}

void FileWriter::runContext()
{
    boost::asio::co_spawn(io, []() -> boost::asio::awaitable<void> { co_return; }(), boost::asio::detached);
    io.run();
}

FileReader::FileReader(
    const std::string& filePath,
    const std::function<char*()>& allocate,
    const std::function<void(char*)>& deallocate,
    const size_t bufferSize)
    : file(filePath + ".dat", std::ios::in | std::ios::binary)
    , keyFile(filePath + "_key.dat", std::ios::in | std::ios::binary)
    , filePath(filePath)
    , readBuffer(allocate())
    , readKeyBuffer(nullptr)
    , readBufferPos(0)
    , readKeyBufferPos(0)
    , readBufferEnd(0)
    , readKeyBufferEnd(0)
    , bufferSize(bufferSize)
    , allocate(allocate)
    , deallocate(deallocate)
{
    if (!file.is_open())
    {
        throw std::ios_base::failure("Failed to open file for reading");
    }
    if (!keyFile.is_open())
    {
        throw std::ios_base::failure("Failed to open key file for reading");
    }
}

FileReader::~FileReader()
{
    file.close();
    deallocate(readBuffer);
    std::filesystem::remove(filePath + ".dat");

    keyFile.close();
    deallocate(readKeyBuffer);
    std::filesystem::remove(filePath + "_key.dat");
}

size_t FileReader::read(void* dest, const size_t size)
{
    return read(dest, size, readBuffer, readBufferPos, readBufferEnd, file);
}

size_t FileReader::readKey(void* dest, const size_t size)
{
    if (readKeyBuffer == nullptr)
    {
        readKeyBuffer = allocate();
    }

    return read(dest, size, readKeyBuffer, readKeyBufferPos, readKeyBufferEnd, keyFile);
}

size_t
FileReader::read(void* dest, const size_t dataSize, char* buffer, size_t& bufferPos, size_t& bufferEnd, std::ifstream& fileStream) const
{
    auto* destPtr = static_cast<char*>(dest);
    if (bufferSize == 0)
    {
        return readFromFile(destPtr, dataSize, fileStream);
    }

    auto totalRead = 0UL;
    while (totalRead < dataSize)
    {
        if (bufferPos == bufferEnd)
        {
            bufferEnd = readFromFile(buffer, bufferSize, fileStream);
            bufferPos = 0;
            if (bufferEnd == 0)
            {
                /// End of file
                break;
            }
        }

        const auto copySize = std::min(dataSize - totalRead, bufferEnd - bufferPos);
        std::memcpy(destPtr, buffer + bufferPos, copySize);
        bufferPos += copySize;
        destPtr += copySize;
        totalRead += copySize;
    }

    return totalRead;
}

size_t FileReader::readFromFile(char* buffer, const size_t dataSize, std::ifstream& fileStream)
{
    fileStream.read(buffer, dataSize);
    if (fileStream.fail() && !fileStream.eof())
    {
        throw std::ios_base::failure("Failed to read from file");
    }
    return fileStream.gcount();
}

}
