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

#ifndef NES_INCLUDE_STATE_STATE_MANAGER_HPP_
#define NES_INCLUDE_STATE_STATE_MANAGER_HPP_

#include <State/StateId.hpp>
#include <State/StateVariable.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <mutex>
#include <unordered_map>

namespace NES {
namespace Runtime {
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
    uint64_t nodeId;

  public:
    explicit StateManager(uint64_t nodeId) { this->nodeId = nodeId; }

    /**
     * Register a new StateVariable object with default value callback
     * @tparam Key
     * @tparam Value
     * @param variable_name an unique identifier for the state variable
     * @param defaultCallback a function that gets called when retrieving a value not present in the state
     * @return the state variable as a reference
     */
    template<typename Key, typename Value>
    StateVariable<Key, Value>* registerStateWithDefault(const StateId& variableStateId,
                                                        std::function<Value(const Key&)>&& defaultCallback) {
        std::unique_lock<std::mutex> lock(mutex);
        auto stateVariable = new StateVariable<Key, Value>(variableStateId, std::move(defaultCallback));
        stateVariables[variableStateId] = stateVariable;
        return stateVariable;
    }

    /**
     * Register a new StateVariable object
     * @tparam Key
     * @tparam Value
     * @param variable_name an unique identifier for the state variable
     * @return the state variable as a reference
     */
    template<typename Key, typename Value>
    StateVariable<Key, Value>* registerState(const StateId& variableStateId) {

        std::unique_lock<std::mutex> lock(mutex);
        auto stateVariable = new StateVariable<Key, Value>(variableStateId);
        NES_ASSERT(stateVariables.find(variableStateId) == stateVariables.end(), "State variable with this id already exists");
        stateVariables[variableStateId] = stateVariable;
        return stateVariable;
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
        NES_ASSERT(variable, "State variable is null");
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

    uint64_t getNodeId() { return this->nodeId; }

    ~StateManager() { destroy(); }

    void destroy() {
        std::unique_lock<std::mutex> lock(mutex);
        for (auto& it : stateVariables) {
            delete it.second;
        }
        stateVariables.clear();
    }
};
}// namespace Runtime
}// namespace NES
#endif// NES_INCLUDE_STATE_STATE_MANAGER_HPP_
