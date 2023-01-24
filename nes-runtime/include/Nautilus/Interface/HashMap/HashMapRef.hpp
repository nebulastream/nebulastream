//
// Created by pgrulich on 23.01.23.
//

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
