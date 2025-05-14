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
#include <boost/asio/write.hpp>

namespace NES::Runtime::Execution
{

boost::asio::io_context& getIoContext()
{
    static boost::asio::io_context ioContext;
    return ioContext;
}

FileWriter::FileWriter(
    const std::string& filePath,
    const std::function<char*()>& allocate,
    const std::function<void(char*)>& deallocate,
    const size_t bufferSize)
    : file(getIoContext())
    , writeBuffer(allocate())
    //, writeKeyBuffer(nullptr)
    , writeBufferPos(0)
    //, writeKeyBufferPos(0)
    , bufferSize(bufferSize)
    , allocate(allocate)
    , deallocate(deallocate)
{
    const auto fd = open((filePath + ".dat").c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd == -1)
    {
        throw std::runtime_error("Failed to open file or key file for writing");
    }

    file.assign(fd);
}

FileWriter::~FileWriter()
{
    /// Buffer needs to be flushed manually before calling the destructor
    // TODO add synchronous flush
    getIoContext().run();
    deallocateBuffers();
    file.close();
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
        co_await flushBuffer();
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
            co_await flushBuffer();
        }
    }
    //co_return;
}

boost::asio::awaitable<void> FileWriter::flush()
{
    if (bufferSize == 0)
    {
        co_return;
    }

    if (writeBufferPos > 0)
    {
        co_await flushBuffer();
    }
    //co_return;
}

void FileWriter::deallocateBuffers()
{
    deallocate(writeBuffer);
    writeBuffer = nullptr;
}

boost::asio::awaitable<void> FileWriter::flushBuffer()
{
    const auto buffer = std::shared_ptr<char>(new char[writeBufferPos], std::default_delete<char[]>());
    std::memcpy(buffer.get(), writeBuffer, writeBufferPos);

    co_await boost::asio::async_write(file, boost::asio::buffer(buffer.get(), writeBufferPos), boost::asio::use_awaitable);

    writeBufferPos = 0;
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
        //throw std::ios_base::failure("Failed to open key file for reading");
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
