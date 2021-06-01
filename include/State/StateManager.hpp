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
#include <State/StateId.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>

namespace NES {
namespace NodeEngine {
/**
 * This class is the entry point for stateful operators that require state
 * This class is used as a singleton and creates StateVariable<K, V>, i.e., mutable data set of key-value pairs.
 * It performs basic garbage colletion of state variables upon termination.
 */

class StateManager {
    using state_variable_base_type = detail::Destroyable*;

  private:
    std::mutex mutex;
    std::unordered_map<StateId, state_variable_base_type> stateVariables;
    uint64_t node_id;

  public:
    explicit StateManager(uint64_t node_id) {
        this->node_id = node_id;
    }

    /**
     * Register a new StateVariable object with default value callback
     * @tparam Key
     * @tparam Value
     * @param variable_name an unique identifier for the state variable
     * @param defaultCallback a function that gets called when retrieving a value not present in the state
     * @return the state variable as a reference
     */
    template<typename Key, typename Value>
    StateVariable<Key, Value>* registerStateWithDefault(const StateId& variableName,
                                                        std::function<Value(const Key&)>&& defaultCallback) {
        std::unique_lock<std::mutex> lock(mutex);
        auto state_var = new StateVariable<Key, Value>(variableName, std::move(defaultCallback));
        stateVariables[variableName] = state_var;
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
    StateVariable<Key, Value>* registerState(const StateId& variableName) {
        
        std::unique_lock<std::mutex> lock(mutex);
        auto state_var = new StateVariable<Key, Value>(variableName);
        stateVariables[variableName] = state_var;
        return state_var;
    }

    /**
     * Remove a StateVariable object
     * @tparam Key
     * @tparam Value
     * @param variable_name an unique identifier for the state variable
     * @return the state variable as a reference
     */
    template<typename Key, typename Value>
    void unRegisterState(StateVariable<Key, Value>* stateVariable) {
        std::unique_lock<std::mutex> lock(mutex);
        NES_ASSERT(stateVariable, "State variable is null");
        for (auto it = stateVariables.begin(), last = stateVariables.end(); it != last; ++it) {
            if (it->second == stateVariable) {
                it = stateVariables.erase(it);
                break;
            }
        }
        delete stateVariable;
    }

    /**
     * Remove a new StateVariable object
     * @tparam Key
     * @tparam Value
     * @param variable_name an unique identifier for the state variable
     * @return the state variable as a reference
     */
    template<typename Key, typename Value>
    void unRegisterState(const StateVariable<Key, Value>* variable) {
        std::unique_lock<std::mutex> lock(mutex);
        //we iterate over all state_variables and remove the variable if existent
        for (auto& [name, stateVar] : stateVariables) {
            if (variable == stateVar) {
                delete stateVar;
                stateVariables.erase(name);
                return;
            }
        }
    }

    const uint64_t getNodeId() {
        return this->node_id;
    }

    ~StateManager() { destroy(); }

    void destroy() {
        std::unique_lock<std::mutex> lock(mutex);
        for (auto& it : stateVariables) {
            delete it.second;
        }
        stateVariables.clear();
    }
};
}// namespace NodeEngine
}// namespace NES
#endif//STATEMANAGER_HPP
