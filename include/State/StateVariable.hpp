
#ifndef STATEVARIABLE_HPP
#define STATEVARIABLE_HPP

#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "libcuckoo/cuckoohash_map.hh"

namespace NES {

namespace detail {

template<typename T>
struct StateVariableEmplaceHelper {
    template<typename... Arguments>
    static T create(Arguments&&... args) {
        return T(std::forward<Arguments>(args)...);// abstracted away with O1+
    }
};
template<typename T>
struct StateVariableEmplaceHelper<T*> {
    template<typename... Arguments>
    static T* create(Arguments&&... args) {
        // TODO change new to something else!
        return new T(std::forward<Arguments>(args)...);
    }
};

template<typename Key, typename T>
struct StateVariableDestroyerHelper {
    static void destroy(cuckoohash_map<Key, T>& backend) {
        auto locked_table = backend.lock_table();
        locked_table.clear();
        locked_table.unlock();
    }
};
template<typename Key, typename T>
struct StateVariableDestroyerHelper<Key, T*> {
    static void destroy(cuckoohash_map<Key, T*>& backend) {
        auto locked_table = backend.lock_table();
        for (auto& it : locked_table) {
            if (it.second != nullptr) {
                delete it.second;
            }
        }
        locked_table.clear();
        locked_table.unlock();
    }
};

class Destroyable {
  public:
    virtual ~Destroyable() {}
};
}// namespace detail

template<
    typename Key,
    typename Value,
    std::enable_if_t<std::is_integral<Value>::value || std::is_pointer<Value>::value, int> = 0>
class StateVariable : public detail::Destroyable {
  private:
    typedef cuckoohash_map<Key, Value> StateBackend;
    typedef StateBackend& StateBackendRef;
    typedef typename cuckoohash_map<Key, Value>::mapped_type StateBackendMappedType;
    typedef typename cuckoohash_map<Key, Value>::locked_table LockedStateBackend;
    typedef LockedStateBackend& LockedStateBackendRef;
    typedef typename LockedStateBackend::const_iterator KeyValueRangeHandleConstIterator;
    typedef typename LockedStateBackend::iterator KeyValueRangeHandleIterator;

  private:
    std::string name;
    StateBackend backend;

  public:
    class KeyValueHandle {
        friend class StateVariable;

      private:
        StateBackendRef backend;
        Key key;

      private:
        explicit KeyValueHandle(StateBackend& backend, Key key) : backend(backend), key(key) {}

      public:
        /**
     * check for existence of a key-value pair
     * @return true if the key-value pair exists
     */
        explicit operator bool() {
            return contains();
        }

        /**
    * check for existence of a key-value pair
    * @return true if the key-value pair exists
    */
        [[nodiscard]] bool contains() const {
            return backend.contains(key);
        }

        /**
     * Retrieves the actual value for a key
     * @return the actual value for a key
     */
        Value value() {
            return backend.find(key);
        }

        /**
     * Retrieves the actual value for a key.
     * If the value is not set yet, we set a default value.
     * @return the actual value for a key
     */
        template<typename... Arguments>
        Value valueOrDefault(Arguments&&... args) {
            if (!backend.contains(key)) {
                emplace(std::forward<Arguments>(args)...);
            }
            return value();
        }

        /**
     * Perform a Read-modify-write of a key-value pair
     * @tparam F an invocable type void(StateBackendMappedType&)
     * @param func an invocable of type void(StateBackendMappedType&)
     * @return true on success
     */
        template<typename F, std::enable_if_t<std::is_invocable<F, void, StateBackendMappedType&>::value, int> = 0>
        bool rmw(F func) {
            return backend.update_fn(key, func);
        }

        /**
     * Set a new value for the key-value pair
     * @param newValue
     * @return self
     */
        KeyValueHandle& operator=(Value&& newValue) {
            emplace(std::move(newValue));
            return *this;
        }

        /**
     * Set a new value for the key-value pair
     * @param newValue
     * @return self
     */
        KeyValueHandle& operator=(Value newValue) {
            emplace(std::move(newValue));
            return *this;
        }

        /**
     * Set a new value for the key-value pair
     * @param newValue
     * @return self
     */
        void put(Value&& newValue) {
            emplace(std::move(newValue));
        }

        /**
     * Set a new value for the key-value pair using emplace semantics
     * @tparam Arguments
     * @param args
     */
        template<typename... Arguments>
        void emplace(Arguments&&... args) {
            backend
                .insert_or_assign(key, detail::StateVariableEmplaceHelper<Value>::create(std::forward<Arguments>(args)...));
        }

        /**
     * Delete a key-value pair
     */
        void clear() {
            backend.erase(key);
        }
    };

    /**
   * Range of key-value pairs. Note that this object locks the range of values, i.e., holds a lock.
   */
    class KeyValueRangeHandle {
        friend class StateVariable;

      private:
        explicit KeyValueRangeHandle(StateBackendRef backend) : backend(backend.lock_table()) {}

      public:
        /**
     *
     * @return a const iterator to the selected range
     */
        KeyValueRangeHandleConstIterator cbegin() {
            return backend.cbegin();
        }

        /**
     *
     * @return a const iterator to the selected range
     */
        KeyValueRangeHandleConstIterator cend() {
            return backend.cend();
        }

        /**
     *
     * @return a iterator to the selected range
     */
        KeyValueRangeHandleIterator begin() {
            return backend.begin();
        }

        /**
     *
     * @return a iterator to the selected range
     */
        KeyValueRangeHandleIterator end() {
            return backend.end();
        }

      private:
        LockedStateBackend backend;
    };

  public:
    explicit StateVariable(std::string name) : name(std::move(name)), backend() {
    }

    explicit StateVariable(const StateVariable<Key, Value>& other) = delete;

    explicit StateVariable(StateVariable<Key, Value>&& other) {
        *this = std::move(other);
    }

    virtual ~StateVariable() override {
        detail::StateVariableDestroyerHelper<Key, Value>::destroy(backend);
    }

    StateVariable& operator=(const StateVariable<Key, Value>& other) = delete;

    StateVariable& operator=(StateVariable<Key, Value>&& other) {
        name = std::move(other.name);
        backend = std::move(other.backend);
        return *this;
    }

  public:
    /**
     * Point lookup of a key-value pair
     * @param key
     * @return an accessor to a key-value pair
     */
    KeyValueHandle get(Key key) {
        return KeyValueHandle(backend, key);
    }

    /**
   * Point lookup of a key-value pair
   * @param key
   * @return an accessor to a key-value pair
   */
    KeyValueHandle operator[](Key&& key) {
        return KeyValueHandle(backend, key);
    }

    /**
   * Point lookup of a key-value pair
   * @param key
   * @return an accessor to a key-value pair
   */
    KeyValueHandle operator[](const Key& key) {

        return KeyValueHandle(backend, key);
    }

    KeyValueRangeHandle range(Key start, Key end) {
        assert(false && "not implemented yet");
    }

    /**
   * Range of all key-value pairs
   * @param key
   * @return an accessor to a key-value pair
   */
    KeyValueRangeHandle rangeAll() {
        return KeyValueRangeHandle(backend);
    }
};
}// namespace NES
#endif//STATEVARIABLE_HPP
