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

#ifndef NES_UNIKERNEL_UTIL_MMAPCIRCULARBUFFER_HPP
#define NES_UNIKERNEL_UTIL_MMAPCIRCULARBUFFER_HPP
#include <span>
class MMapCircularBuffer {
  public:
    explicit MMapCircularBuffer(size_t capacity);

    ~MMapCircularBuffer();

    std::span<char> reserveDataForWrite(size_t requested_size);

    void returnMemoryForWrite(std::span<char> reserved, size_t used_bytes);

    /**
     * \brief 
     * \param requested_size May return less than the requested size 
     * \return returns a view into the popped data.
     *         NOTE: this data may be overridden by subsequent writes 
     */
    std::span<const char> popData(size_t requested_size);
    std::span<const char> peekData(size_t requested_size);

    size_t size() const;

    size_t getCapacity() const;
    bool full() const;
    bool empty() const;

  private:
    char* data;
    int fd;
    size_t capacity = 0;
    size_t read = 0;
    size_t write = 0;
};
#endif//NES_UNIKERNEL_UTIL_MMAPCIRCULARBUFFER_HPP
