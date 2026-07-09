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

#include <Util/CcxTopology.hpp>

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

std::mutex overrideMutex;
std::string overrideString; /// NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool instanceMaterialized = false; /// NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

size_t parseCpuId(const std::string_view token)
{
    size_t value = 0;
    const auto [ptr, ec] = std::from_chars(token.data(), token.data() + token.size(), value);
    if (ec != std::errc{} || ptr != token.data() + token.size())
    {
        throw InvalidConfigParameter("CcxTopology: expected a cpu id, got '{}'", std::string(token));
    }
    return value;
}

/// Parses one "0-5,24-29"-style cpu list (the sysfs shared_cpu_list format) into cpu ids.
std::vector<size_t> parseCpuList(const std::string_view list)
{
    std::vector<size_t> cpus;
    size_t pos = 0;
    while (pos <= list.size())
    {
        const auto comma = std::min(list.find(',', pos), list.size());
        const std::string_view entry = list.substr(pos, comma - pos);
        if (entry.empty())
        {
            throw InvalidConfigParameter("CcxTopology: empty entry in cpu list '{}'", std::string(list));
        }
        if (const auto dash = entry.find('-'); dash != std::string_view::npos)
        {
            const auto first = parseCpuId(entry.substr(0, dash));
            const auto last = parseCpuId(entry.substr(dash + 1));
            if (last < first)
            {
                throw InvalidConfigParameter("CcxTopology: descending range '{}'", std::string(entry));
            }
            for (size_t cpu = first; cpu <= last; ++cpu)
            {
                cpus.push_back(cpu);
            }
        }
        else
        {
            cpus.push_back(parseCpuId(entry));
        }
        if (comma == list.size())
        {
            break;
        }
        pos = comma + 1;
    }
    return cpus;
}

}

CcxTopology::CcxTopology(const std::vector<std::vector<size_t>>& groups)
{
    ccxCount = std::max<size_t>(1, groups.size());
    ccxGroups = groups;
    size_t maxCpu = 0;
    for (const auto& group : groups)
    {
        for (const auto cpu : group)
        {
            maxCpu = std::max(maxCpu, cpu);
        }
    }
    cpuToCcx.assign(maxCpu + 1, 0);
    for (uint32_t ccx = 0; ccx < groups.size(); ++ccx)
    {
        for (const auto cpu : groups[ccx])
        {
            cpuToCcx[cpu] = ccx;
        }
    }
}

size_t CcxTopology::ioThreadCpu(const size_t ioIndex) const
{
    const auto& group = ccxGroups[ioIndex % ccxGroups.size()];
    return group[(ioIndex / ccxGroups.size()) % group.size()];
}

size_t CcxTopology::workerCpu(const size_t workerIndex, const size_t numIoThreads) const
{
    const size_t ccx = workerIndex % ccxGroups.size();
    const auto& group = ccxGroups[ccx];
    /// Workers fill each CCX's cpu slots AFTER the io threads striped into that CCX
    /// (numIoThreads/numCcx per CCX, +1 for the first numIoThreads%numCcx CCXs).
    const size_t ioSlotsInCcx = (numIoThreads / ccxGroups.size()) + (ccx < numIoThreads % ccxGroups.size() ? 1 : 0);
    return group[(ioSlotsInCcx + workerIndex / ccxGroups.size()) % group.size()];
}

CcxTopology CcxTopology::fromString(const std::string_view topology)
{
    std::vector<std::vector<size_t>> groups;
    size_t pos = 0;
    while (pos <= topology.size())
    {
        const auto semi = std::min(topology.find(';', pos), topology.size());
        const std::string_view group = topology.substr(pos, semi - pos);
        if (group.empty())
        {
            throw InvalidConfigParameter("CcxTopology: empty group in '{}'", std::string(topology));
        }
        groups.push_back(parseCpuList(group));
        if (semi == topology.size())
        {
            break;
        }
        pos = semi + 1;
    }
    if (groups.empty())
    {
        throw InvalidConfigParameter("CcxTopology: empty topology string");
    }
    return CcxTopology{groups};
}

CcxTopology CcxTopology::detectFromSysfs()
{
    const size_t numCpus = std::max<size_t>(1, std::thread::hardware_concurrency());
    std::unordered_map<std::string, uint32_t> groupIdByList;
    std::vector<std::vector<size_t>> groups;
    for (size_t cpu = 0; cpu < numCpus; ++cpu)
    {
        std::ifstream file(fmt::format("/sys/devices/system/cpu/cpu{}/cache/index3/shared_cpu_list", cpu));
        std::string list;
        if (!file || !std::getline(file, list) || list.empty())
        {
            NES_WARNING("CcxTopology: could not read L3 shared_cpu_list for cpu {}; falling back to a single CCX", cpu);
            return CcxTopology{};
        }
        if (const auto [it, inserted] = groupIdByList.try_emplace(list, static_cast<uint32_t>(groups.size())); inserted)
        {
            try
            {
                groups.push_back(parseCpuList(list));
            }
            catch (const Exception&)
            {
                NES_WARNING("CcxTopology: could not parse sysfs cpu list '{}'; falling back to a single CCX", list);
                return CcxTopology{};
            }
        }
    }
    return CcxTopology{groups};
}

void CcxTopology::setOverride(const std::string_view topology)
{
    if (topology.empty())
    {
        return;
    }
    const std::scoped_lock lock(overrideMutex);
    if (instanceMaterialized)
    {
        NES_WARNING("CcxTopology: override '{}' ignored, topology already materialized", std::string(topology));
        return;
    }
    overrideString = std::string(topology);
}

const CcxTopology& CcxTopology::instance()
{
    static const CcxTopology topology = []
    {
        const std::scoped_lock lock(overrideMutex);
        instanceMaterialized = true;
        if (const char* env = std::getenv("NES_CCX_TOPOLOGY"); env != nullptr && *env != '\0')
        {
            return fromString(env);
        }
        if (!overrideString.empty())
        {
            return fromString(overrideString);
        }
        return detectFromSysfs();
    }();
    return topology;
}

}
