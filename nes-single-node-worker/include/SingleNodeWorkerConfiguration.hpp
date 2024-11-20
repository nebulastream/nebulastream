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
#include <string>
#include <Configurations/PrintingVisitor.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>

namespace NES::Configuration
{

class SingleNodeWorkerConfiguration final : public Configurations::BaseConfiguration
{
public:
    /// GRPC Server Address URI. By default, it binds to any address and listens on port 8080
    Configurations::StringOption grpcAddressUri
        = {"grpc",
           "[::]:8080",
           R"(The address to try to bind to the server in URI form. If
the scheme name is omitted, "dns:///" is assumed. To bind to any address,
please use IPv6 any, i.e., [::]:<port>, which also accepts IPv4
connections.  Valid values include dns:///localhost:1234,
192.168.1.1:31416, dns:///[::1]:27182, etc.)"};

protected:
    std::vector<BaseOption*> getOptions() override { return {&workerConfiguration, &grpcAddressUri}; }
    template <typename T>
    friend void generateHelp(std::ostream& ostream);

public:
    SingleNodeWorkerConfiguration() = default;
    Configurations::WorkerConfiguration workerConfiguration = {"worker", "NodeEngine Configuration"};
};
}
