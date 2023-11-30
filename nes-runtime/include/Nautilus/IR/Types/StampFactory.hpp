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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_STAMPFACTORY_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_STAMPFACTORY_HPP_

#include <memory>
namespace NES::Nautilus::IR::Types {
class Stamp;
using StampPtr = std::shared_ptr<Stamp>;
class StampFactory {
  public:
    static StampPtr createVoidStamp();
    static StampPtr createUInt8Stamp();
    static StampPtr createUInt16Stamp();
    static StampPtr createUInt32Stamp();
    static StampPtr createUInt64Stamp();
    static StampPtr createInt8Stamp();
    static StampPtr createInt16Stamp();
    static StampPtr createInt32Stamp();
    static StampPtr createInt64Stamp();
    static StampPtr createBooleanStamp();
    static StampPtr createFloatStamp();
    static StampPtr createDoubleStamp();
    static StampPtr createAddressStamp();
    static StampPtr createArrayStamp(uint64_t size, StampPtr component);
};

}// namespace NES::Nautilus::IR::Types

#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_STAMPFACTORY_HPP_
