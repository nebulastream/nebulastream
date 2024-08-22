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

#pragma once

namespace NES::Sources
{

///-todo: explain why forward ref
class SourceRegistry;

/// Auto generated, injects all plugins into the SourceRegistry
class GeneratedSourceRegistrar
{
public:
    GeneratedSourceRegistrar() = default;
    ~GeneratedSourceRegistrar() = default;

    static void registerAllPlugins(SourceRegistry& registry)
    {
        /// External register functions
        ///-todo

        /// Internal register functions
        RegisterTCPSource(registry);
        RegisterCSVSource(registry);
    }

private:
    /// External Registry Header Functions
    ///-todo

    /// Internal Registry Header Functions
    static void RegisterTCPSource(SourceRegistry& registry);
    static void RegisterCSVSource(SourceRegistry& registry);
};

}