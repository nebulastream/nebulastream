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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_HASHMAPREF_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_HASHMAPREF_HPP_
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
namespace NES::Nautilus::Interface {

class HashMapRef {
  public:
    class EntryRef;
    using EntryRefPtr = std::unique_ptr<EntryRef>;
    class EntryRef {
      public:
        virtual EntryRefPtr getNext() = 0;
        virtual Value<MemRef> getKeyPtr() = 0;
        virtual Value<MemRef> getValuePtr() = 0;
    };
    virtual EntryRefPtr findOrCreate(std::vector<Value<>> keys) = 0;
    virtual EntryRefPtr findOne(std::vector<Value<>> keys) = 0;
    virtual EntryRefPtr insertEntry(std::vector<Value<>> keys) = 0;
};

}// namespace NES::Nautilus::Interface

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_HASHMAPREF_HPP_
