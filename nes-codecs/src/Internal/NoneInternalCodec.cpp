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

#include <Internal/NoneInternalCodec.hpp>

#include <cstddef>
#include <memory>

#include <Interface/RecordLayoutUtil.hpp>
#include <InternalCodec.hpp>
#include <InternalCodecRegistry.hpp>

namespace NES
{

size_t NoneEncodedValue::getTotalSizeInBytes() const
{
    return valueType.getSizeInBytesWithNull();
}

void NoneEncodedValue::writeEncodedValue(const nautilus::val<int8_t*>& address, const VarSizedStoreFn& storeVarSized) const
{
    /// Directly write the wrapped, un-encoded value to the buffer address.
    writeVarVal(valueType, address, value, storeVarSized);
}

std::unique_ptr<EncodedValue> NoneInternalCodec::encodeValue(const VarVal value)
{
    /// Simply wrap value into NoneEncodedValue
    return std::make_unique<NoneEncodedValue>(value, inputType);
}

std::unique_ptr<InternalCodec> NoneInternalCodec::provideCodec(InternalCodecRegistryArguments args)
{
    return std::make_unique<NoneInternalCodec>(args.fieldType);
}
}
