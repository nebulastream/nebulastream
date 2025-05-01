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

#include <condition_variable>
#include <memory>
#include <utility>


struct Channel;
class Ingestion;

class Valve
{
    std::shared_ptr<Channel> channel;
    explicit Valve(std::shared_ptr<Channel> channel) : channel(std::move(channel)) { }

public:
    ~Valve();

    Valve(const Valve& other) = delete;
    Valve(Valve&& other) noexcept;
    Valve& operator=(const Valve& other) = delete;
    Valve& operator=(Valve&& other) noexcept;

    bool apply_pressure();
    bool release_pressure();

    friend std::pair<Valve, Ingestion> Backpressure();
};

class Ingestion
{
    std::shared_ptr<Channel> channel;
    explicit Ingestion(std::shared_ptr<Channel> channel) : channel(std::move(channel)) { }

public:
    void wait(const std::stop_token& stopToken) const;

    friend std::pair<Valve, Ingestion> Backpressure();
};

std::pair<Valve, Ingestion> Backpressure();
