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

#include <chrono>
#include <cstddef>
#include <optional>
#include <vector>
#include <Runtime/TupleBuffer.hpp>

/**
 * This enum reflects the different types of buffer managers in the system
 * global: overall buffer manager
 * local: buffer manager that we give to the processing
 * fixed: buffer manager that we use for sources
 */
namespace NES::Memory
{
enum class BufferManagerType : uint8_t
{
    GLOBAL,
    LOCAL,
    FIXED
};

class AbstractBufferProvider
{
public:
    virtual ~AbstractBufferProvider()
    {
        /// nop
    }

    virtual void destroy() = 0;

    virtual size_t getAvailableBuffers() const = 0;

    virtual BufferManagerType getBufferManagerType() const = 0;

    /**
     * @return Configured size of the buffers
     */
    virtual size_t getBufferSize() const = 0;

    /**
     * @return Number of total buffers in the pool
     */
    virtual size_t getNumOfPooledBuffers() const = 0;

    /**
     * @return number of unpooled buffers
     */
    virtual size_t getNumOfUnpooledBuffers() const = 0;

    /**
     * @brief Provides a new TupleBuffer. This blocks until a buffer is available.
     * @return a new buffer
     */
    virtual TupleBuffer getBufferBlocking() = 0;

    /**
     * @brief Returns a new TupleBuffer wrapped in an optional or an invalid option if there is no buffer.
     * @return a new buffer
     */
    virtual std::optional<TupleBuffer> getBufferNoBlocking() = 0;

    /**
     * @brief Returns a new Buffer wrapped in an optional or an invalid option if there is no buffer available within
     * timeout_ms.
     * @param timeout_ms the amount of time to wait for a new buffer to be retuned
     * @return a new buffer
     */
    virtual std::optional<TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds timeout_ms) = 0;

    /**
     * @brief Returns an unpooled buffer of size bufferSize wrapped in an optional or an invalid option if an error
     * occurs.
     * @param bufferSize
     * @return a new buffer
     */
    virtual std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize) = 0;
};

class AbstractPoolProvider
{
public:
    /**
      * @brief Create a fixed buffer manager that is assigned to one pipeline or thread
      * @param numberOfReservedBuffers number of exclusive buffers to give to the pool
      * @return a local buffer manager with numberOfReservedBuffers exclusive buffer
      */
    virtual std::optional<std::shared_ptr<AbstractBufferProvider>> createFixedSizeBufferPool(size_t numberOfReservedBuffers) = 0;
};

/// free functions for buffer provider

/**
 * @brief This function allocates a TupleBuffer of a desired size via a provider
 * @param provider the buffer allocator
 * @param size the desired size
 * @return a tuplebuffer of size size allocated via the given provider
 */
TupleBuffer allocateVariableLengthField(std::shared_ptr<AbstractBufferProvider> provider, uint32_t size);

}
