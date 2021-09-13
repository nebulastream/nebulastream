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

//
// Created by pgrulich on 23.08.21.
//

#ifndef NES_INCLUDE_QUERYCOMPILER_INTERPRETER_DATATYPES_NESINT64_HPP_
#define NES_INCLUDE_QUERYCOMPILER_INTERPRETER_DATATYPES_NESINT64_HPP_

#include <QueryCompiler/Interpreter/Values/NesValue.hpp>
#include <QueryCompiler/Interpreter/Values/NesBool32.hpp>
namespace NES::QueryCompilation {

class NesInt64 : public NesValue  {
  public:
    NesInt64(int64_t value);
    NesValuePtr equals(NesValuePtr) const override;
    int64_t getValue() const;
    operator bool() override { return NesValue::operator bool(); }
    void write(NesMemoryAddressPtr memoryAddress) const override;
    NesValuePtr mul(NesValuePtr ptr) const override;
    NesValuePtr add(NesValuePtr ptr) const override;

  private:
    int64_t value;
};

}// namespace NES::QueryCompiler
#endif//NES_INCLUDE_QUERYCOMPILER_INTERPRETER_DATATYPES_NESINT64_HPP_
