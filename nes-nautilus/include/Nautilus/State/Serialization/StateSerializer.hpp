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
#include <rfl/capnproto.hpp>
#include <cstring>
#include <stdexcept>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>

namespace NES::Serialization {

using TupleBuffer = Memory::TupleBuffer;
using AbstractBufferProvider = Memory::AbstractBufferProvider;

template<typename T>
class StateSerializer {
public:
    static TupleBuffer serialize(const T& state, AbstractBufferProvider* bufferProvider) {
        auto bytes = rfl::capnproto::write(state);

        // Allocate TupleBuffer for serialized data
        auto bufferOpt = bufferProvider->getUnpooledBuffer(bytes.size());
        if (!bufferOpt) {
            throw std::runtime_error("Failed to allocate buffer for serialization");
        }
        
        auto buffer = bufferOpt.value();
        std::memcpy(buffer.getBuffer(), bytes.data(), bytes.size());
        
        return buffer;
    }
    
    static rfl::Result<T> deserialize(const TupleBuffer& buffer) {
        return rfl::capnproto::read<T>(
            reinterpret_cast<const char*>(buffer.getBuffer<uint8_t>()),
            buffer.getBufferSize());
    }
    
    static rfl::Result<T> view(const TupleBuffer& buffer) {
        return deserialize(buffer);
    }
};

template<typename StateType>
class StateView {
public:
    explicit StateView(const TupleBuffer& buffer) {
        auto result = StateSerializer<StateType>::view(buffer);
        if (result) {
            state = result.value();
            valid = true;
        } else {
            valid = false;
        }
    }
    
    const StateType* operator->() const { 
        return valid ? &state : nullptr; 
    }
    
    const StateType& operator*() const { 
        return state; 
    }
    
    bool isValid() const { 
        return valid; 
    }
    
private:
    StateType state;
    bool valid;
};

}
