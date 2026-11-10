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

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>
#include <sys/types.h>

/// Pins the threads of a worker to CPUs, if enabled (query_engine.cpu_pinning). Performance threads (the worker threads, and the
/// threads of sources that ask for it) get a CPU of their own on the performance CPUs, one per physical core before any hyperthread
/// sibling; once all are taken, a performance thread may run on any of them. Every other NES thread runs on the misc CPUs, which the
/// scheduler balances among them.
/// By default, the performance CPUs are the P-cores and the misc CPUs the E-cores of a hybrid CPU (/sys/devices/cpu_core/cpus and
/// /sys/devices/cpu_atom/cpus); without E-cores both are all CPUs. Both are limited to the CPUs the process may run on.
namespace NES::CpuPinning
{

enum class Role : uint8_t
{
    Misc,
    Performance,
};

/// Parses a CPU list as Linux writes it, e.g. "0-3,8,10-11". nullopt if it is malformed.
std::optional<std::vector<int>> parseCpuList(std::string_view list);

/// Enables pinning for the threads that start or call pinThisThread from now on. Empty lists select the defaults. The first call
/// wins; a second worker in the same process shares its CPUs.
void enable(std::vector<int> performanceCpus, std::vector<int> miscCpus);

/// Pins the calling thread to the CPUs of its role. A no-op while pinning is disabled.
void pinThisThread(Role role);

/// Gives a child process the CPUs the worker started with, as it would otherwise keep the pinning of the thread that started it.
void unpinProcess(pid_t pid);

/// Pins a child process to `cpu`, whether or not pinning is enabled. Returns false if the process may not run there.
bool pinProcess(pid_t pid, int cpu);

}
