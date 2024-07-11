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
#ifndef NES_EXECUTION_INCLUDE_EXECUTION_MEMORYPROVIDER_MEMORYPROVIDER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_MEMORYPROVIDER_MEMORYPROVIDER_HPP_

#include <Nautilus/DataTypes/ExecutableDataType.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

class TupleBufferMemoryProvider;
using MemoryProviderPtr = std::unique_ptr<TupleBufferMemoryProvider>;

class TupleBufferMemoryProvider {
  public:
    virtual ~TupleBufferMemoryProvider();

    static MemoryProviderPtr createMemoryProvider(const uint64_t bufferSize, const SchemaPtr schema);

    virtual MemoryLayouts::MemoryLayoutPtr getMemoryLayoutPtr() = 0;

    virtual Nautilus::Record read(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                                  nautilus::val<int8_t*>& bufferAddress,
                                  nautilus::val<uint64_t>& recordIndex) const = 0;

    virtual void
    write(nautilus::val<uint64_t>& recordIndex, nautilus::val<int8_t*>& bufferAddress, NES::Nautilus::Record& rec) const = 0;

    static Nautilus::ExecDataType
    load(const PhysicalTypePtr& type, nautilus::val<int8_t*>& bufferReference, nautilus::val<int8_t*>& fieldReference);

    static Nautilus::ExecDataType store(const NES::PhysicalTypePtr& type,
                                        nautilus::val<int8_t*>& bufferReference,
                                        nautilus::val<int8_t*>& fieldReference,
                                        Nautilus::ExecDataType value);

    [[nodiscard]] bool includesField(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                                     const Nautilus::Record::RecordFieldIdentifier& fieldIndex) const;
};

}// namespace NES::Runtime::Execution::MemoryProvider
#endif// NES_EXECUTION_INCLUDE_EXECUTION_MEMORYPROVIDER_MEMORYPROVIDER_HPP_
