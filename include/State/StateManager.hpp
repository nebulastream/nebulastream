
#ifndef STATEMANAGER_HPP
#define STATEMANAGER_HPP

#include <State/StateVariable.hpp>
#include <mutex>
#include <unordered_map>
namespace NES {

/**
 * This class is the entry point for stateful operators that require state
 * This class is used as a singleton and creates StateVariable<K, V>, i.e., mutable data set of key-value pairs.
 * It performs basic garbage colletion of state variables upon termination.
 */
class StateManager {
    typedef detail::Destroyable* state_variable_base_type;

  private:
    std::mutex mutex;
    std::unordered_map<std::string, state_variable_base_type> state_variables;

    ~StateManager() {
        std::unique_lock<std::mutex> lock(mutex);
        for (auto& it : state_variables) {
            delete it.second;
        }
    }

  public:
    /**
     * Register a new StateVariable object
     * @tparam Key
     * @tparam Value
     * @param variable_name an unique identifier for the state variable
     * @return the state variable as a reference
     */
    template<typename Key, typename Value>
    StateVariable<Key, Value>& registerState(const std::string& variable_name) {
        std::unique_lock<std::mutex> lock(mutex);
        auto state_var = new StateVariable<Key, Value>(variable_name);
        state_variables[variable_name] = state_var;
        return *state_var;
    }

    /**
     * Singleton accessor to the state manager. As of now, there is one per process.
     */
    static StateManager& instance();
};
}// namespace NES
#endif//STATEMANAGER_HPP
