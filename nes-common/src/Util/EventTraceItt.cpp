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


#ifdef NES_EVENT_TRACE_ITT

    #include <array>
    #include <cstddef>
    #include <cstdint>
    #include <cstring>
    #include <string>
    #include <string_view>
    #include <tuple>
    #include <type_traits>
    #include <unordered_map>
    #include <vector>
    #include <ittnotify.h>
    #include <rfl/to_view.hpp>
    #include <EventTraceBackends.hpp>

namespace NES::EventTrace::Itt
{
namespace
{

/// A field of an event type, added as task metadata when a span ends.
struct FieldInfo
{
    __itt_string_handle* name;
    uint32_t offset;
    bool isKey;
    bool isSigned;
};

struct TypeInfo
{
    __itt_domain* domain = nullptr;
    __itt_string_handle* name = nullptr;
    std::vector<FieldInfo> fields;
};

/// VTune groups tasks by domain, so the category decides the domain.
__itt_domain* domainOf(Category category)
{
    static const std::array<__itt_domain*, 3> domains{
        __itt_domain_create("NebulaStream.Work"), __itt_domain_create("NebulaStream.Wait"), __itt_domain_create("NebulaStream.Other")};
    return domains.at(static_cast<size_t>(category));
}

template <typename Event>
TypeInfo describe()
{
    TypeInfo info{.domain = domainOf(Event::category), .name = __itt_string_handle_create(std::string(Event::name).c_str()), .fields = {}};
    if constexpr (!std::is_empty_v<Event>)
    {
        Event instance{};
        rfl::to_view(instance).apply(
            [&](const auto& field)
            {
                using Field = std::remove_cvref_t<decltype(*field.value())>;
                const auto* address
                    = reinterpret_cast<const std::byte*>(field.value()); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                info.fields.push_back(
                    {.name = __itt_string_handle_create(std::string(field.name()).c_str()),
                     .offset = static_cast<uint32_t>(
                         address - reinterpret_cast<const std::byte*>(&instance)), /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                     .isKey = std::is_same_v<Field, Key>,
                     .isSigned = std::is_same_v<Field, int64_t>});
            });
    }
    return info;
}

const std::vector<TypeInfo>& types()
{
    static const std::vector<TypeInfo> instance = []<typename... Events>(std::tuple<Events...>*)
    { return std::vector<TypeInfo>{describe<Events>()...}; }(static_cast<EventTypes*>(nullptr));
    return instance;
}

/// Only accessed on the sampler thread with its lock held, see EventTraceBackends.hpp.
std::unordered_map<uint64_t, __itt_counter>& counters()
{
    static std::unordered_map<uint64_t, __itt_counter> instance;
    return instance;
}

void addMetadata(const TypeInfo& info, const std::byte* payload)
{
    for (const auto& field : info.fields)
    {
        if (field.isKey)
        {
            Key key;
            std::memcpy(&key, payload + field.offset, sizeof(key));
            std::array<uint64_t, 3> values{key.origin, key.seq, key.chunk};
            __itt_metadata_add(info.domain, __itt_null, field.name, __itt_metadata_u64, values.size(), values.data());
            continue;
        }
        uint64_t value = 0;
        std::memcpy(&value, payload + field.offset, sizeof(value));
        __itt_metadata_add(info.domain, __itt_null, field.name, field.isSigned ? __itt_metadata_s64 : __itt_metadata_u64, 1, &value);
    }
}
}

void record(uint16_t type, Kind kind, const void* payload, uint32_t) noexcept
{
    if (type == typeId<CounterSample>)
    {
        CounterSample sample;
        std::memcpy(&sample, payload, sizeof(sample));
        if (const auto it = counters().find(sample.counter); it != counters().end())
        {
            __itt_counter_set_value(it->second, &sample.value);
        }
        return;
    }
    const auto& info = types().at(type);
    switch (kind)
    {
        case Kind::Begin:
            __itt_task_begin(info.domain, __itt_null, __itt_null, info.name);
            return;
        case Kind::End:
            /// Metadata attaches to the current task, which is the one this call ends. The end record holds the final values.
            addMetadata(info, static_cast<const std::byte*>(payload));
            __itt_task_end(info.domain);
            return;
        case Kind::Instant:
            __itt_marker(info.domain, __itt_null, info.name, __itt_marker_scope_thread);
            return;
    }
}

void counterCreated(uint64_t id, std::string_view name)
{
    counters()[id] = __itt_counter_create_typed(std::string(name).c_str(), "NebulaStream", __itt_metadata_s64);
}

void counterDestroyed(uint64_t id)
{
    if (const auto it = counters().find(id); it != counters().end())
    {
        __itt_counter_destroy(it->second);
        counters().erase(it);
    }
}

}

#endif
