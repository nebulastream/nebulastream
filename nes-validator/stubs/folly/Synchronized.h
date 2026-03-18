#pragma once
// Minimal folly::Synchronized stub for WASM build
// Replaces thread-safe wrapper with a simple non-synchronized wrapper
// (WASM is single-threaded, so no synchronization needed)
#include <mutex>
#include <utility>

namespace folly {

template<typename T, typename Mutex = std::mutex>
class Synchronized {
public:
    Synchronized() = default;
    explicit Synchronized(T t) : data_(std::move(t)) {}

    // Read-only access
    template<typename F>
    auto withRLock(F&& f) const -> decltype(f(std::declval<const T&>())) {
        return f(data_);
    }

    // Read-write access
    template<typename F>
    auto withWLock(F&& f) -> decltype(f(std::declval<T&>())) {
        return f(data_);
    }

    // Copy out
    T copy() const { return data_; }

    // Lock proxy for -> access
    class LockedPtr {
    public:
        explicit LockedPtr(T& data) : data_(data) {}
        T* operator->() const { return &data_; }
        T& operator*() const { return data_; }
    private:
        T& data_;
    };

    class ConstLockedPtr {
    public:
        explicit ConstLockedPtr(const T& data) : data_(data) {}
        const T* operator->() const { return &data_; }
        const T& operator*() const { return data_; }
    private:
        const T& data_;
    };

    LockedPtr wlock() { return LockedPtr(data_); }
    ConstLockedPtr rlock() const { return ConstLockedPtr(data_); }
    LockedPtr lock() { return LockedPtr(data_); }

private:
    T data_;
};

} // namespace folly
