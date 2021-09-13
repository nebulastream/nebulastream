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

#ifndef NES_INCLUDE_QUERYCOMPILER_INTERPRETER_DATATYPES_NESINTU8_HPP_
#define NES_INCLUDE_QUERYCOMPILER_INTERPRETER_DATATYPES_NESINTU8_HPP_


#include <QueryCompiler/Interpreter/Values/NesValue.hpp>
#include <QueryCompiler/Interpreter/Values/NesBool32.hpp>
namespace NES::QueryCompilation {

class NesUInt8 : public NesValue  {
  public:
    NesUInt8(uint8_t value);
    NesValuePtr equals(NesValuePtr) const override;
    uint8_t getValue() const;
    NesValuePtr lt(NesValuePtr ptr) const override;
    NesValuePtr mul(NesValuePtr ptr) const override;
    NesValuePtr add(NesValuePtr ptr) const override;
    NesValuePtr le(NesValuePtr ptr) const override;
    NesValuePtr sub(NesValuePtr ptr) const override;
    operator bool() override { return NesValue::operator bool(); }
    void write(NesMemoryAddressPtr) const override;

  private:
    uint8_t value;
};

}// namespace NES::QueryCompiler

#endif//NES_INCLUDE_QUERYCOMPILER_INTERPRETER_DATATYPES_NESINTU8_HPP_
