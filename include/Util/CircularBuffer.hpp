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

#ifndef NES_CIRCULARBUFFER_H_
#define NES_CIRCULARBUFFER_H_

#include <memory>
#include <type_traits>
namespace NES {

namespace detail {

// true if const iterator, false otherwise
template<class, bool>
class CircularBufferIterator;

/**
 * @brief The CircularBufferIterator class holds all iterator
 * related typedefs and overloaded operators. The template
 * takes as argument the const-ness (true/false) to avoid
 * having many different types of iterators with the same
 * functionality.
 *
 * @tparam cbT type of circular buffer
 * @tparam isConst is/is-not const
 */
template<class cbT, bool isConst>
class CircularBufferIterator {
  public:
    using type = CircularBufferIterator<cbT, isConst>;
    using value_type = typename cbT::value_type;
    using difference_type = std::ptrdiff_t;
    using pointer = typename std::conditional_t<isConst, const value_type, value_type>*;
    using reference = typename std::conditional_t<isConst, const value_type, value_type>&;
    using iterator_category = std::random_access_iterator_tag;

    CircularBufferIterator() = default;

    reference operator*() const noexcept { return container->at(idx); }
    CircularBufferIterator& operator++() noexcept {
        ++idx;
        return *this;
    }

    friend CircularBufferIterator operator-(CircularBufferIterator it, int i) noexcept {
        it -= i;
        return it;
    }
    friend CircularBufferIterator& operator-=(CircularBufferIterator& it, int i) noexcept {
        it.idx -= i;
        return it;
    }
    template<bool C>
    bool operator==(const CircularBufferIterator<cbT, C>& rhs) const noexcept { return idx == rhs.idx; }
    template<bool C>
    bool operator!=(const CircularBufferIterator<cbT, C>& rhs) const noexcept { return idx != rhs.idx; }

  private:
    // reuse typenames
    friend cbT;
    using size_type = typename cbT::size_type;
    CircularBufferIterator(size_type idx, std::conditional_t<isConst, const cbT, cbT>* rv) noexcept
        : idx(idx), container(rv){};
    size_type idx;

    // non-const/const representation of container
    std::conditional_t<isConst, const cbT, cbT>* container;
};// class CircularBufferIterator
}// namespace detail

/**
 * @brief A templated class for a circular buffer. The implementation
 * is header-only. Currently the structure supports push/emplace
 * at front and checks for full/capacity/size. The iterators are
 * only forward. Addition of elements is at the front. Removal
 * is at the back.
 *
 * Refs:
 * - https://www.boost.org/doc/libs/1_74_0/doc/html/boost/circular_buffer.html
 * - https://embeddedartistry.com/blog/2017/05/17/creating-a-circular-buffer-in-c-and-c
 * - https://www.approxion.com/category/circular-adventures/page/1/
 *
 * @tparam T - type of the value in the buffer slots.
 */
template<
    class T,
    typename Allocator = std::allocator<T>,
    std::enable_if_t<std::is_integral<T>::value || std::is_pointer<T>::value, int> = 0>
class CircularBuffer {
  public:
    // STL-style typedefs, similar to std::deque
    using value_type = T;
    using allocator_type = Allocator;
    using size_type = std::size_t;
    using pointer = T*;
    using reference = T&;
    using const_reference = const T&;
    using difference_type = std::ptrdiff_t;
    using iterator = detail::CircularBufferIterator<CircularBuffer, false>;
    using const_iterator = detail::CircularBufferIterator<CircularBuffer, true>;

    /**
     * @brief The ctor of the circ buffer, takes a size parameter.
     * @param size of the internal buffer
     */
    explicit CircularBuffer(uint64_t size) : maxSize(size),
                                             currentSize(0),
                                             head(0),
                                             buffer(std::make_unique<T[]>(size)){};

    // copy and move
    CircularBuffer(const CircularBuffer& other) = delete;
    CircularBuffer(CircularBuffer&& other) noexcept = default;

    CircularBuffer& operator=(const CircularBuffer&) = delete;
    CircularBuffer& operator=(CircularBuffer&&) noexcept = default;

    ~CircularBuffer() = default;

    // front/end, access begin or end ptr
    reference front() noexcept { return *begin(); }
    reference back() noexcept { return *(end() - 1); }
    const_reference front() const noexcept { return *begin(); }
    const_reference back() const noexcept { return *(end() - 1); }

    // size-capacity
    [[nodiscard]] size_type size() const noexcept { return currentSize; }
    [[nodiscard]] size_type capacity() const noexcept { return maxSize; }
    [[nodiscard]] bool empty() const { return currentSize == 0; }
    [[nodiscard]] bool full() const { return maxSize == currentSize; }

    // iterators
    iterator begin() noexcept { return iterator(0, this); }
    iterator end() noexcept { return iterator(size(), this); }
    const_iterator cbegin() const noexcept { return const_iterator(0, this); }
    const_iterator cend() const noexcept { return const_iterator(size(), this); }
    const_iterator begin() const noexcept { return cbegin(); }
    const_iterator end() const noexcept { return cend(); }

    // random-access: [] and at
    reference operator[](size_type idx) { return buffer[(head + idx) % maxSize]; }
    reference at(size_type idx) { return buffer[(head + idx) % maxSize]; }
    const_reference operator[](const size_type idx) const { return buffer[(head + idx) % maxSize]; }
    const_reference at(const size_type idx) const { return buffer[(head + idx) % maxSize]; }

    // modifiers: push and emplace front
    template<bool b = true, typename = std::enable_if_t<b && std::is_copy_assignable<T>::value>>
    void push(const T& value) noexcept(std::is_nothrow_copy_assignable<T>::value) {
        if (full()) {
            decrementHead();
        } else {
            decrementHeadIncSize();
        }
        front_() = value;
    }

    template<bool b = true, typename = std::enable_if_t<b && std::is_move_assignable<T>::value>>
    void push(T&& value) noexcept(std::is_nothrow_move_assignable<T>::value) {
        if (full()) {
            decrementHead();
        } else {
            decrementHeadIncSize();
        }
        front_() = std::move(value);
    }

    template<typename... Args>
    void emplace(Args&&... args) noexcept(std::is_nothrow_constructible<T, Args...>::value&& std::is_nothrow_move_assignable<T>::value) {
        if (full()) {
            decrementHead();
        } else {
            decrementHeadIncSize();
        }
        front_() = T(std::forward<Args>(args)...);
    }

    // modifier: pop only in back
    T pop() {
        if (empty()) {
            return T();
        }
        auto& elt = back_();
        decrementSize();
        return std::move(elt);
    }

  private:
    // indicates writes
    uint64_t head;

    // maximum size of buffer
    size_type maxSize;

    // current size of buffer
    size_type currentSize;

    // the buffer, of type T[]
    std::unique_ptr<T[]> buffer;

    // front and back, with wrap-around, for assignment
    reference front_() noexcept { return buffer[head]; }
    const_reference front_() const noexcept { return buffer[head]; }
    reference back_() noexcept { return buffer[(head + currentSize - 1) % maxSize]; }
    const_reference back_() const noexcept { return buffer[(head + currentSize - 1) % maxSize]; }

    void incrementHeadDecrSize() noexcept {
        head = (head + 1) % maxSize;
        --currentSize;
    }
    void decrementHeadIncSize() noexcept {
        head = (head + maxSize - 1) % maxSize;
        ++currentSize;
    }
    void incrementSize() noexcept {
        ++currentSize;
    }
    void decrementSize() noexcept {
        --currentSize;
    }
    void incrementHead() noexcept {
        head = (head + 1) % maxSize;
    }
    void decrementHead() noexcept {
        head = (head + maxSize - 1) % maxSize;
    }

};// class CircularBuffer
}// namespace NES
#endif /* INCLUDE_CIRCULARBUFFER_H_ */
