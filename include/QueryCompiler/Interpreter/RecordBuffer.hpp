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

#ifndef NES_INCLUDE_QUERYCOMPILER_INTERPRETER_RECORDBUFFER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_INTERPRETER_RECORDBUFFER_HPP_
#include <QueryCompiler/Interpreter/Values/NesValue.hpp>
#include <QueryCompiler/Interpreter/Record.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <QueryCompiler/Interpreter/ForwardDeclaration.hpp>
#include <API/Schema.hpp>
#include <memory>

namespace NES::Runtime::DynamicMemoryLayout {
class DynamicMemoryLayout;
using DynamicMemoryLayoutPtr = std::shared_ptr<DynamicMemoryLayout>;
}
namespace NES::Runtime{
class TupleBuffer;
using TupleBufferPtr = std::shared_ptr<TupleBuffer>;
}
namespace NES::QueryCompilation {
class RecordBuffer {
  public:
    RecordBuffer(Runtime::TupleBuffer buffer, SchemaPtr schema);
    virtual ~RecordBuffer() = default;
    virtual NesInt32Ptr getNumberOfTuples();
    virtual NesMemoryAddressPtr getBufferAddress();
    virtual RecordPtr operator[](std::size_t index) const;

  private:
    Runtime::TupleBuffer buffer;
    SchemaPtr schema;
};

}// namespace NES::QueryCompilation

#endif//NES_INCLUDE_QUERYCOMPILER_INTERPRETER_RECORDBUFFER_HPP_
