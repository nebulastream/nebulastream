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
        NES_DEBUG("~StateManager()");
        std::unique_lock<std::mutex> lock(mutex);
        for (auto& it : state_variables) {
            delete it.second;
        }
        state_variables.clear();
    }

  public:
    /**
     * Register a new StateVariable object with default value callback
     * @tparam Key
     * @tparam Value
     * @param variable_name an unique identifier for the state variable
     * @param defaultCallback a function that gets called when retrieving a value not present in the state
     * @return the state variable as a reference
     */
    template<typename Key, typename Value>
    StateVariable<Key, Value>* registerStateWithDefault(const std::string& variable_name, std::function<Value(const Key&)>&& defaultCallback) {
        std::unique_lock<std::mutex> lock(mutex);
        auto state_var = new StateVariable<Key, Value>(variable_name, std::move(defaultCallback));
        state_variables[variable_name] = state_var;
        return state_var;
    }

    /**
     * Register a new StateVariable object
     * @tparam Key
     * @tparam Value
     * @param variable_name an unique identifier for the state variable
     * @return the state variable as a reference
     */
    template<typename Key, typename Value>
    StateVariable<Key, Value>* registerState(const std::string& variableName) {
        std::unique_lock<std::mutex> lock(mutex);
        auto state_var = new StateVariable<Key, Value>(variableName);
        state_variables[variableName] = state_var;
        return state_var;
    }

    /**
     * Register a new StateVariable object
     * @tparam Key
     * @tparam Value
     * @param variable_name an unique identifier for the state variable
     * @return the state variable as a reference
     */
    void unRegisterState(const std::string& variableName) {
        std::unique_lock<std::mutex> lock(mutex);
        delete state_variables[variableName];
        state_variables.erase(variableName);
    }

    /**
     * Singleton accessor to the state manager. As of now, there is one per process.
     */
    static StateManager& instance();
};
}// namespace NES
#endif//STATEMANAGER_HPP
