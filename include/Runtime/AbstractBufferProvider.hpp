/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef NES_INCLUDE_NODEENGINE_AbstractBufferProvider_HPP_
#define NES_INCLUDE_NODEENGINE_AbstractBufferProvider_HPP_

#include <cstddef>

/**
 * This enum reflects the different types of buffer managers in the system
 * global: overall buffer manager
 * local: buffer manager that we give to the processing
 * fixed: buffer manager that we use for sources
 */
enum BufferManagerType { GLOBAL, LOCAL, FIXED };
class AbstractBufferProvider {
  public:
    virtual ~AbstractBufferProvider() {
        // nop
    }

    virtual void destroy() = 0;

    virtual size_t getAvailableBuffers() const = 0;

    virtual BufferManagerType getBufferManagerType() const = 0;
};

#endif//NES_INCLUDE_NODEENGINE_AbstractBufferProvider_HPP_
