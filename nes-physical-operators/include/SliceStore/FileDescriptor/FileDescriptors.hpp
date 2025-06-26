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

#include <fstream>
#include <functional>
#include <Runtime/AbstractBufferProvider.hpp>
#include <boost/asio.hpp>

namespace NES
{

enum class FileLayout : uint8_t
{
    NO_SEPARATION,
    SEPARATE_PAYLOAD,
    SEPARATE_KEYS
};

class FileWriter
{
public:
    FileWriter(
        boost::asio::io_context& ioCtx,
        const std::string& filePath,
        const std::function<char*()>& allocate,
        const std::function<void(char*)>& deallocate,
        const std::function<void()>& onDestruct,
        size_t bufferSize);
    ~FileWriter();

    bool initialize();

    boost::asio::awaitable<void> write(const void* data, size_t size);
    boost::asio::awaitable<void> writeKey(const void* data, size_t size);
    boost::asio::awaitable<uint32_t> writeVarSized(const void* data);

    boost::asio::awaitable<void> flush();
    void flushAndDeallocateBuffers();
    void deallocateBuffers();

private:
    static boost::asio::awaitable<void> flushBuffer(boost::asio::posix::stream_descriptor& stream, const char* buffer, size_t& size);

    boost::asio::io_context& ioCtx;
    boost::asio::posix::stream_descriptor file;
    boost::asio::posix::stream_descriptor keyFile;

    char* writeBuffer;
    char* writeKeyBuffer;
    size_t writeBufferPos;
    size_t writeKeyBufferPos;
    size_t bufferSize;

    uint32_t varSizedCnt;
    std::string filePath;
    std::function<char*()> allocate;
    std::function<void(char*)> deallocate;
    std::function<void()> onDestruct;
};

class FileReader
{
public:
    FileReader(
        const std::string& filePath,
        const std::function<char*()>& allocate,
        const std::function<void(char*)>& deallocate,
        size_t bufferSize);
    ~FileReader();

    size_t read(void* dest, size_t size);
    size_t readKey(void* dest, size_t size);
    Memory::TupleBuffer readVarSized(Memory::AbstractBufferProvider* bufferProvider, uint32_t idx) const;

private:
    size_t read(void* dest, size_t dataSize, char* buffer, size_t& bufferPos, size_t& bufferEnd, std::ifstream& fileStream) const;
    static size_t readFromFile(char* buffer, size_t dataSize, std::ifstream& fileStream);

    std::ifstream file;
    std::ifstream keyFile;

    char* readBuffer;
    char* readKeyBuffer;
    size_t readBufferPos;
    size_t readKeyBufferPos;
    size_t readBufferEnd;
    size_t readKeyBufferEnd;
    size_t bufferSize;

    std::string filePath;
    std::function<char*()> allocate;
    std::function<void(char*)> deallocate;
};

}
