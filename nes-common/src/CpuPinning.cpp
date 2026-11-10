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

#include <CpuPinning.hpp>

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <fstream>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <sched.h>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <sys/types.h>

namespace NES::CpuPinning
{
namespace
{

struct State
{
    std::mutex mutex;
    bool enabled = false;
    /// One per physical core first, then their hyperthread siblings
    std::vector<int> performance;
    std::vector<bool> taken;
    std::vector<int> misc;
    cpu_set_t original{};
};

State& state()
{
    static State instance;
    return instance;
}

/// The performance CPU a thread holds, released when the thread ends
struct Slot
{
    std::optional<size_t> index;

    void release()
    {
        if (index)
        {
            auto& global = state();
            const std::scoped_lock lock(global.mutex);
            global.taken[*index] = false;
            index.reset();
        }
    }

    Slot() = default;
    Slot(const Slot&) = delete;
    Slot& operator=(const Slot&) = delete;
    Slot(Slot&&) = delete;
    Slot& operator=(Slot&&) = delete;

    ~Slot() { release(); }
};

thread_local Slot slot;

std::optional<std::vector<int>> readCpuList(const std::string& path)
{
    std::ifstream file(path);
    std::string line;
    if (!file || !std::getline(file, line))
    {
        return std::nullopt;
    }
    return parseCpuList(line);
}

/// 0 for the first hardware thread of a core, 1 for its sibling, and so on
int siblingIndex(const int cpu)
{
    const auto siblings = readCpuList(fmt::format("/sys/devices/system/cpu/cpu{}/topology/thread_siblings_list", cpu));
    if (!siblings)
    {
        return 0;
    }
    return static_cast<int>(std::ranges::count_if(*siblings, [cpu](const int sibling) { return sibling < cpu; }));
}

std::vector<int> toList(const cpu_set_t& set)
{
    std::vector<int> cpus;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
    {
        if (CPU_ISSET(cpu, &set))
        {
            cpus.push_back(cpu);
        }
    }
    return cpus;
}

cpu_set_t toSet(const std::vector<int>& cpus)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    for (const int cpu : cpus)
    {
        CPU_SET(cpu, &set);
    }
    return set;
}

void setAffinity(const std::vector<int>& cpus)
{
    const auto set = toSet(cpus);
    if (sched_setaffinity(0, sizeof(set), &set) != 0)
    {
        NES_WARNING("Could not pin the thread to CPUs {}", fmt::join(cpus, ","));
    }
}

}

std::optional<std::vector<int>> parseCpuList(const std::string_view list)
{
    std::vector<int> cpus;
    for (const auto part : list | std::views::split(','))
    {
        const std::string_view range(part.begin(), part.end());
        if (range.empty())
        {
            continue;
        }
        const auto dash = range.find('-');
        const auto first = range.substr(0, dash);
        const auto last = dash == std::string_view::npos ? first : range.substr(dash + 1);
        int from = 0;
        int to = 0;
        if (std::from_chars(first.data(), first.data() + first.size(), from).ptr != first.data() + first.size()
            || std::from_chars(last.data(), last.data() + last.size(), to).ptr != last.data() + last.size() || from < 0 || to < from
            || to >= CPU_SETSIZE)
        {
            return std::nullopt;
        }
        for (int cpu = from; cpu <= to; ++cpu)
        {
            cpus.push_back(cpu);
        }
    }
    std::ranges::sort(cpus);
    const auto [end, _] = std::ranges::unique(cpus);
    cpus.erase(end, cpus.end());
    return cpus;
}

void enable(std::vector<int> performanceCpus, std::vector<int> miscCpus)
{
    auto& global = state();
    const std::scoped_lock lock(global.mutex);
    if (global.enabled)
    {
        return;
    }
    sched_getaffinity(0, sizeof(global.original), &global.original);
    const auto allowed = [&](std::vector<int> cpus)
    { return cpus | std::views::filter([&](const int cpu) { return CPU_ISSET(cpu, &global.original); }) | std::ranges::to<std::vector>(); };

    const auto all = toList(global.original);
    if (performanceCpus.empty())
    {
        performanceCpus = readCpuList("/sys/devices/cpu_core/cpus").value_or(all);
    }
    if (miscCpus.empty())
    {
        miscCpus = readCpuList("/sys/devices/cpu_atom/cpus").value_or(std::vector<int>{});
    }
    global.performance = allowed(std::move(performanceCpus));
    global.misc = allowed(std::move(miscCpus));
    if (global.performance.empty())
    {
        global.performance = all;
    }
    if (global.misc.empty())
    {
        global.misc = global.performance;
    }
    std::ranges::stable_sort(global.performance, {}, siblingIndex);
    global.taken.assign(global.performance.size(), false);
    global.enabled = true;
    NES_INFO(
        "Pinning worker threads to CPUs {} and other threads to CPUs {}", fmt::join(global.performance, ","), fmt::join(global.misc, ","));
}

void pinThisThread(const Role role)
{
    auto& global = state();
    slot.release();
    const std::scoped_lock lock(global.mutex);
    if (!global.enabled)
    {
        return;
    }
    if (role == Role::Misc)
    {
        setAffinity(global.misc);
        return;
    }
    if (const auto free = std::ranges::find(global.taken, false); free != global.taken.end())
    {
        *free = true;
        slot.index = static_cast<size_t>(free - global.taken.begin());
        setAffinity({global.performance[*slot.index]});
        return;
    }
    /// ponytail: more performance threads than CPUs share all of them; per-CPU load balancing if that ever matters
    setAffinity(global.performance);
}

void unpinProcess(const pid_t pid)
{
    auto& global = state();
    const std::scoped_lock lock(global.mutex);
    if (global.enabled && sched_setaffinity(pid, sizeof(global.original), &global.original) != 0)
    {
        NES_WARNING("Could not unpin process {}", pid);
    }
}

bool pinProcess(const pid_t pid, const int cpu)
{
    const auto set = toSet({cpu});
    if (sched_setaffinity(pid, sizeof(set), &set) != 0)
    {
        NES_WARNING("Could not pin process {} to CPU {}", pid, cpu);
        return false;
    }
    return true;
}

}
