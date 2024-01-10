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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_LIST_LIST_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_LIST_LIST_HPP_

#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/List/ListValue.hpp>
#include <Nautilus/Interface/DataTypes/TypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
namespace NES::Nautilus {

/**
 * @brief Base type for all lists. A list can have different values that all have the same underlying type.
 * LISTs are typically used to store arrays of numbers.
 */
class List : public Nautilus::Any {
  public:
    static const inline auto type = TypeIdentifier::create<List>();

    /**
     * @brief Iterator over all entries in the list.
     */
    class ListValueIterator {
      public:
        ListValueIterator(List& listRef, Value<UInt32>& currentIndex);
        ListValueIterator& operator++();
        bool operator==(const ListValueIterator& other) const;
        Value<> operator*();

      private:
        List& list;
        Value<UInt32> currentIndex;
    };

    explicit List(const TypeIdentifier* childType) : Any(childType){};

    /**
     * @brief Return the length of the list.
     * @return Value<Int32> as length.
     */
    virtual Value<UInt32> length() = 0;

    /**
     * @brief Checks if this lists is equal to another list.
     * Two lists are equal if they contain equal elements of the same type.
     * @param otherList
     * @return Value<Boolean>
     */
    virtual Value<Boolean> equals(const Value<List>& otherList) = 0;

    /**
    * @brief Reads one element from the text value at a specific index.
    * @param index as Value<Int32>
    * @return Value<>
    */
    virtual Value<> read(Value<UInt32>& index) const = 0;

    /**
     * @brief Writes one element a specific index.
     * @param index as Value<Int32>
     * @param value as Value<>
     */
    virtual void write(Value<UInt32>& index, const Value<>& value) = 0;

    ListValueIterator begin();
    ListValueIterator end();

    ~List() override;
};

/**
 * @brief Checks if T is a valid list component type.
 * Currently, lists are restricted to Ints and Floats.
 * @tparam T
 */
template<typename T>
concept IsListComponentType = std::is_base_of_v<Int, T> || std::is_same_v<Float, T> || std::is_same_v<T, Double>;

/**
 * @brief A typed list that contains values of a specific nautilus data types.
 * @tparam BaseType
 */
template<IsListComponentType BaseType>
class TypedList final : public List {
  public:
    static const inline auto type = TypeIdentifier::create<TypedList<BaseType>>();
    /**
     * @brief Exposes the component type of this TypedList.
     */
    using ComponentType = typename BaseType::RawType;
    /**
     * @brief Exposes the raw type of this TypedList.
     */
    using RawType = ListValue<ComponentType>;

    /**
     * @brief Constructor to create a typed list from a reference to the correct raw type.
     * @param ref
     */
    explicit TypedList(TypedRef<RawType> ref);

    /**
     * @brief Return the length of the list.
     * @return Value<Int32> as length.
     */
    Value<UInt32> length() override;

    /**
    * @brief Checks if this lists is equal to another list.
    * Two lists are equal if they contain equal elements of the same type.
    * @param otherList
    * @return Value<Boolean>
    */
    Value<Boolean> equals(const Value<List>& otherList) override;

    /**
    * @brief Reads one element from the text value at a specific index.
    * @param index as Value<Int32>
    * @return Value<>
    */
    Value<> read(Value<UInt32>& index) const override;

    /**
     * @brief Writes one element a specific index.
     * @param index as Value<Int32>
     * @param value as Value<>
     */
    void write(Value<UInt32>& index, const Value<>& value) override;

    AnyPtr copy() override;

  private:
    const TypedRef<RawType> rawReference;
};

}// namespace NES::Nautilus
#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_LIST_LIST_HPP_
