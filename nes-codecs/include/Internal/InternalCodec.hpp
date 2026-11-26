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
#include <cstddef>

#include <Interface/RecordLayoutUtil.hpp>

namespace NES
{

/// A VarVal or composition of VarVals holding a single encoded value.
class EncodedValue
{
public:
    EncodedValue() = default;
    virtual ~EncodedValue() = default;

    /// Returns the total amount of bytes that the encoded value will take up within a TupleBuffer.
    [[nodiscard]] virtual size_t getTotalSizeInBytes() const = 0;

    /// Writes the composite elements of the encoded value into the buffer at the provided address.
    virtual void writeEncodedValue(const nautilus::val<int8_t*>& address, const VarSizedStoreFn& storeVarSized) const = 0;
};

/// Holds the state of an InternalCodec object.
/// There are two reasons why we choose to implement this as separate class:
/// 1. Clean separation between a Codec and its state. Codecs might also share the same state, for example, different implementations of the same underlying Codec.
/// 2. Sometimes, part of the state or the whole state is needed for random-access of decoding of encoded values. In this case, the state needs to provide a function to serialize itself and write itself into a buffer (tbd).
class InternalCodecState
{
public:
    InternalCodecState() = default;
    virtual ~InternalCodecState() = default;

    /// Returns the total amount of bytes that the serialized state will take up within a TupleBuffer.
    [[nodiscard]] virtual size_t getTotalSizeInBytes() const = 0;
};

class InternalCodec
{
public:
    InternalCodec() = default;
    virtual ~InternalCodec() = default;

    /// Encodes the value within the VarVal on-the-fly, based on the Codec and the CodecState.
    /// Returns an EncodedValue object, encapsulating all the VarVals the encoded value is composed of.
    virtual EncodedValue encodeValue(const VarVal value) = 0;
};
}
