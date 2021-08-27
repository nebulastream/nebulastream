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

#ifndef NES_INCLUDE_QUERYCOMPILER_INTERPRETER_DATATYPES_NESINT32_HPP_
#define NES_INCLUDE_QUERYCOMPILER_INTERPRETER_DATATYPES_NESINT32_HPP_


#include <QueryCompiler/Interpreter/Values/NesValue.hpp>
#include <QueryCompiler/Interpreter/Values/NesBool32.hpp>
namespace NES::QueryCompilation {

class NesInt32 : public NesValue  {
  public:
    NesInt32(int32_t value);
    NesValuePtr equals(NesValuePtr) const override;
    int32_t getValue() const;
    operator bool() override { return NesValue::operator bool(); }

  private:
    int32_t value;
};

}// namespace NES::QueryCompiler

#endif//NES_INCLUDE_QUERYCOMPILER_INTERPRETER_DATATYPES_NESINT16_HPP_
