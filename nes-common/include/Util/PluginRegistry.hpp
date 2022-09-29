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

#ifndef NES_UTIL_HPP_
#define NES_UTIL_HPP_
#include <list>
#include <memory>

namespace NES::Util {

/**
 * @brief The plugin registry allows the dynamic registration of plugins at runtime.
 * A plugin is a provider of a specific type T, which defines the plugin interface.
 * Plugins use [[maybe_unused]] static T::Add<PluginXType> pluginX; to register them self to the plugin.
 * @tparam T plugin interface type
 */
template<typename T>
class PluginRegistry {

  private:
    static inline std::list<std::unique_ptr<T>> items = std::list<std::unique_ptr<T>>();

  public:
    static std::list<std::unique_ptr<T>>& getPlugins() { return items; }
    /** A static registration template. Use like such:
    *
    * Registry<PluginInterfaceType>::Add<PluginType> X;
    *
    * Use of this template requires that:
    *
    * 1. The registered subclass has a default constructor.
    */
    template<typename V>
    class Add {
        static std::unique_ptr<T> CtorFn() { return std::make_unique<V>(); }

      public:
        Add() { PluginRegistry<T>::items.emplace_back(CtorFn()); }
    };
};
}// namespace NES::Util

#endif//NES_UTIL_HPP_
