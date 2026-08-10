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
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>
#include <Configuration/WorkerConfiguration.hpp>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/EndpointValidation.hpp>

namespace NES
{

class SingleNodeWorkerConfiguration final : public BaseConfiguration
{
public:
    ScalarOption<std::string> dataAddress = {"data_address", "Data-plane address. This is the {Hostname}:{PORT}"};

    /// GRPC Server Address URI. By default, it binds to any address and listens on port 8080
    ScalarOption<std::string> grpcAddressUri
        = {"grpc",
           "[::]:8080",
           R"(The address to try to bind to the server in URI form. If
the scheme name is omitted, "dns:///" is assumed. To bind to any address,
please use IPv6 any, i.e., [::]:<port>, which also accepts IPv4
connections.  Valid values include dns:///localhost:1234,
192.168.1.1:31416, dns:///[::1]:27182, etc.)",
           {std::make_shared<EndpointValidation>(EndpointValidation::GRPC)}};

    /// Enable Google Event Trace logging (Chrome tracing format)
    BoolOption enableGoogleEventTrace
        = {"enable_event_trace",
           "false",
           "Enable Google Event Trace logging that generates Chrome tracing compatible JSON files for performance analysis."};

    /// Publish query engine task events into an in-process feed, so that they can be read by an InProcess source
    BoolOption enableTaskStatistics
        = {"enable_task_statistics",
           "false",
           "Publish query engine task events as CSV rows into an in-process feed, which a physical source of type "
           "InProcess can read to make engine statistics queryable."};

    ScalarOption<std::string> taskStatisticsFeed
        = {"task_statistics_feed",
           "engine_events",
           "Name of the in-process feed that task statistics are published to. A physical source of type InProcess "
           "reads the feed of the same name."};

protected:
    std::vector<BaseOption*> getOptions() override;

    template <typename T>
    friend void generateHelp(std::ostream& ostream);

public:
    SingleNodeWorkerConfiguration() = default;
    WorkerConfiguration workerConfiguration = {"worker", "NodeEngine Configuration"};
};
}
