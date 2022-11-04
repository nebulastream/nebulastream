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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

Nautilus::Value<> MemoryProvider::load(PhysicalTypePtr type, Nautilus::Value<Nautilus::MemRef> memRef) {
    if (type->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(type);
        switch (basicType->nativeType) {
            case BasicPhysicalType::INT_8: {
                return memRef.load<Nautilus::Int8>();
            };
            case BasicPhysicalType::INT_16: {
                return memRef.load<Nautilus::Int16>();
            };
            case BasicPhysicalType::INT_32: {
                return memRef.load<Nautilus::Int32>();
            };
            case BasicPhysicalType::INT_64: {
                return memRef.load<Nautilus::Int64>();
            };
            case BasicPhysicalType::UINT_8: {
                return memRef.load<Nautilus::UInt8>();
            };
            case BasicPhysicalType::UINT_16: {
                return memRef.load<Nautilus::UInt16>();
            };
            case BasicPhysicalType::UINT_32: {
                return memRef.load<Nautilus::UInt32>();
            };
            case BasicPhysicalType::UINT_64: {
                return memRef.load<Nautilus::UInt64>();
            };
            case BasicPhysicalType::FLOAT: {
                return memRef.load<Nautilus::Float>();
            };
            case BasicPhysicalType::DOUBLE: {
                return memRef.load<Nautilus::Double>();
            };
            default: {
                NES_ERROR("MemoryProvider::load: Physical Type: " << type << " is currently not supported");
                NES_NOT_IMPLEMENTED();
            };
        }
    }
    NES_NOT_IMPLEMENTED();
}

bool MemoryProvider::includesField(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                            Nautilus::Record::RecordFieldIdentifier fieldIndex) {
    if (projections.empty()) {
        return true;
    }
    return std::find(projections.begin(), projections.end(), fieldIndex) != projections.end();
}

MemoryProvider::~MemoryProvider() {}

} //namespace