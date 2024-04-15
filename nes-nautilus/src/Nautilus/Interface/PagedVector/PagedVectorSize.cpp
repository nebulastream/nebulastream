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

#include <cstdint>
#ifndef UNIKERNEL_LIB
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#endif

#include <Nautilus/Interface/PagedVector/PagedVectorSize.hpp>

namespace NES::Nautilus::Interfaces {

#ifndef UNIKERNEL_LIB
PagedVectorSize::PagedVectorSize(const Schema& schema, size_t pageSize)
    : pageSize_(pageSize), size_(0), schemaSize_(schema.getSchemaSizeInBytes()) {
    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema.fields) {
        if (auto fieldType = field->getDataType(); fieldType->isText()) {
            auto varSizedDataEntryMapKeySize = sizeof(uint64_t);
            size_ += varSizedDataEntryMapKeySize;
        } else {
            size_ += physicalDataTypeFactory.getPhysicalType(fieldType)->size();
        }
    }
}
#endif
PagedVectorSize::PagedVectorSize(size_t page_size, size_t size, size_t schema_size)
    : pageSize_(page_size), size_(size), schemaSize_(schema_size) {}
size_t PagedVectorSize::size() const { return size_; }
size_t PagedVectorSize::schemaSize() const { return schemaSize_; }
size_t PagedVectorSize::pageSize() const { return pageSize_; }
}// namespace NES::Nautilus::Interfaces
