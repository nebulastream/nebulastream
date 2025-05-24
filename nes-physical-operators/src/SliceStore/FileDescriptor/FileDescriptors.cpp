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

#include <filesystem>
#include <SliceStore/FileDescriptor/FileDescriptors.hpp>

namespace NES
{

FileWriter::FileWriter(
    boost::asio::io_context& ioCtx,
    const std::string& filePath,
    const std::function<char*()>& allocate,
    const std::function<void(char*)>& deallocate,
    const size_t bufferSize)
    : ioCtx(ioCtx)
    , file(ioCtx)
    , keyFile(ioCtx)
    , writeBuffer(allocate())
    , writeKeyBuffer(nullptr)
    , writeBufferPos(0)
    , writeKeyBufferPos(0)
    , bufferSize(bufferSize)
    , allocate(allocate)
    , deallocate(deallocate)
{
    const auto fd = open((filePath + ".dat").c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    const auto fdKey = open((filePath + "_key.dat").c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0 || fdKey < 0)
    {
        throw std::runtime_error("Failed to open file or key file for writing");
    }

    file.assign(fd);
    keyFile.assign(fdKey);
}

FileWriter::~FileWriter()
{
    /// Buffer needs to be flushed manually before calling the destructor
    deallocateBuffers();
    file.close();
    keyFile.close();
}

boost::asio::awaitable<void> FileWriter::write(const void* data, size_t size)
{
    if (writeBuffer == nullptr)
    {
        writeBuffer = allocate();
    }

    const auto* dataPtr = static_cast<const char*>(data);
    if (bufferSize == 0)
    {
        co_await flushBuffer(file, writeBuffer, writeBufferPos);
        co_return;
    }

    while (size > 0)
    {
        const auto copySize = std::min(size, bufferSize - writeBufferPos);
        std::memcpy(writeBuffer + writeBufferPos, dataPtr, copySize);
        writeBufferPos += copySize;
        dataPtr += copySize;
        size -= copySize;

        if (writeBufferPos == bufferSize)
        {
            co_await flushBuffer(file, writeBuffer, writeBufferPos);
        }
    }
    co_return;
}

boost::asio::awaitable<void> FileWriter::writeKey(const void* data, size_t size)
{
    if (writeKeyBuffer == nullptr)
    {
        writeKeyBuffer = allocate();
    }

    const auto* dataPtr = static_cast<const char*>(data);
    if (bufferSize == 0)
    {
        co_await flushBuffer(keyFile, writeKeyBuffer, writeKeyBufferPos);
        co_return;
    }

    while (size > 0)
    {
        const auto copySize = std::min(size, bufferSize - writeKeyBufferPos);
        std::memcpy(writeKeyBuffer + writeKeyBufferPos, dataPtr, copySize);
        writeKeyBufferPos += copySize;
        dataPtr += copySize;
        size -= copySize;

        if (writeKeyBufferPos == bufferSize)
        {
            co_await flushBuffer(keyFile, writeKeyBuffer, writeKeyBufferPos);
        }
    }
    co_return;
}

boost::asio::awaitable<void> FileWriter::flush()
{
    if (bufferSize == 0)
    {
        co_return;
    }

    if (writeBuffer != nullptr && writeBufferPos > 0)
    {
        co_await flushBuffer(file, writeBuffer, writeBufferPos);
    }
    if (writeKeyBuffer != nullptr && writeKeyBufferPos > 0)
    {
        co_await flushBuffer(keyFile, writeKeyBuffer, writeKeyBufferPos);
    }
    co_return;
}

void FileWriter::flushAndDeallocateBuffers()
{
    std::promise<void> promise;
    const std::future<void> future = promise.get_future();

    // TODO schedule the awaitable flush to run on the same io context
    boost::asio::post(
        ioCtx,
        [this, &promise]
        {
            try
            {
                boost::asio::co_spawn(
                    ioCtx,
                    flush(),
                    [&promise](const std::exception_ptr& e)
                    {
                        if (e)
                        {
                            promise.set_exception(e);
                        }
                        else
                        {
                            promise.set_value();
                        }
                    });
            }
            catch ([[maybe_unused]] const std::exception& e)
            {
                promise.set_exception(std::current_exception());
            }
        });

    /// Use non-blocking calls to wait for the flush operation to complete
    while (future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready)
    {
        ioCtx.poll();
    }

    deallocateBuffers();
}

void FileWriter::deallocateBuffers()
{
    deallocate(writeBuffer);
    writeBuffer = nullptr;
    deallocate(writeKeyBuffer);
    writeKeyBuffer = nullptr;
}

boost::asio::awaitable<void> FileWriter::flushBuffer(boost::asio::posix::stream_descriptor& stream, const char* buffer, size_t& size)
{
    const auto newBuffer = std::shared_ptr<char>(new char[size], std::default_delete<char[]>());
    std::memcpy(newBuffer.get(), buffer, size);

    co_await boost::asio::async_write(stream, boost::asio::buffer(newBuffer.get(), size), boost::asio::use_awaitable);

    size = 0;
    co_return;
}

FileReader::FileReader(
    const std::string& filePath,
    const std::function<char*()>& allocate,
    const std::function<void(char*)>& deallocate,
    const size_t bufferSize)
    : file(filePath + ".dat", std::ios::in | std::ios::binary)
    , keyFile(filePath + "_key.dat", std::ios::in | std::ios::binary)
    , readBuffer(allocate())
    , readKeyBuffer(nullptr)
    , readBufferPos(0)
    , readKeyBufferPos(0)
    , readBufferEnd(0)
    , readKeyBufferEnd(0)
    , bufferSize(bufferSize)
    , filePath(filePath)
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
