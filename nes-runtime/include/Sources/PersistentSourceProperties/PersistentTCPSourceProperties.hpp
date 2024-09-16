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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_PERSISTENTSOURCEPROPERTIES_PERSISTENTTCPSOURCEPROPERTIES_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_PERSISTENTSOURCEPROPERTIES_PERSISTENTTCPSOURCEPROPERTIES_HPP_

#include <Sources/PersistentSourceProperties/BasePersistentSourceProperties.hpp>
#include <memory>

class MMapCircularBuffer;

namespace NES {
class PersistentTCPSourceProperties : public BasePersistentSourceProperties {

  public:
    /**
     * @brief Ctor
     * @param socketFd: socket file descriptor
     * @param circularBuffer : circular buffer
     */
    PersistentTCPSourceProperties(uint64_t socketFd, const std::shared_ptr<MMapCircularBuffer>& circularBuffer)
        : socketFd(socketFd), circularBuffer(circularBuffer){};

    //Socket file descriptor
    uint64_t socketFd;
    //Circular buffer
    std::shared_ptr<MMapCircularBuffer> circularBuffer;
};
}// namespace NES
#endif// NES_RUNTIME_INCLUDE_SOURCES_PERSISTENTSOURCEPROPERTIES_PERSISTENTTCPSOURCEPROPERTIES_HPP_
