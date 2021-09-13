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
// Created by pgrulich on 31.08.21.
//

#ifndef NES_INCLUDE_QUERYCOMPILER_INTERPRETER_FORWARDDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_INTERPRETER_FORWARDDECLARATION_HPP_
#include <memory>
namespace NES::QueryCompilation{

class NesBool;
class NesMemoryAddress;
class NesUInt8;
class NesInt32;
class NesInt64;
class NesValue;
using NesValuePtr = std::shared_ptr<NesValue>;
using NesBoolPtr = std::shared_ptr<NesBool>;
using NesInt32Ptr = std::shared_ptr<NesInt32>;
using NesInt64Ptr = std::shared_ptr<NesInt64>;
using NesUInt8Ptr = std::shared_ptr<NesUInt8>;
using NesMemoryAddressPtr = std::shared_ptr<NesMemoryAddress>;

class Record;
using RecordPtr = std::shared_ptr<Record>;

}

#endif//NES_INCLUDE_QUERYCOMPILER_INTERPRETER_FORWARDDECLARATION_HPP_
