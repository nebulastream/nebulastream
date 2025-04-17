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
#include <memory>
#include <string>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceDescriptor.hpp>

// namespace NES
// {
//
// /// Container for storing all configurations for physical source
// class PhysicalSource
// {
// public:
//     explicit PhysicalSource(uint64_t id, const Sources::SourceDescriptor& sourceDescriptor, WorkerId workerId, const LogicalSource& logicalSource);
//     ~PhysicalSource() = default;
//
//     PhysicalSource(const PhysicalSource& other) = default;
//     PhysicalSource(PhysicalSource&& other) noexcept = default;
//     PhysicalSource& operator=(const PhysicalSource& other) = default;
//     PhysicalSource& operator=(PhysicalSource&& other) noexcept = default;
//     [[nodiscard]] std::shared_ptr<const Sources::SourceDescriptor> getSourceDescriptor() const;
//     // [[nodiscard]] std::shared_ptr<const Sources::PhysicalSourceDescriptor> createSourceDescriptor(Schema schema);
//
//     [[nodiscard]] uint64_t getID() const;
//
//     friend bool operator==(const PhysicalSource& lhs, const PhysicalSource& rhs) { return lhs.id == rhs.id; }
//     friend bool operator!=(const PhysicalSource& lhs, const PhysicalSource& rhs) { return !(lhs == rhs); }
//     friend bool operator<(const PhysicalSource& lhs, const PhysicalSource& rhs) { return lhs.id < rhs.id; }
//     friend bool operator<=(const PhysicalSource& lhs, const PhysicalSource& rhs) { return rhs >= lhs; }
//     friend bool operator>(const PhysicalSource& lhs, const PhysicalSource& rhs) { return rhs < lhs; }
//     friend bool operator>=(const PhysicalSource& lhs, const PhysicalSource& rhs) { return !(lhs < rhs); }
//
// private:
//
//     uint64_t id;
//     WorkerId workerID;
//
//     std::shared_ptr<Sources::SourceDescriptor> sourceDescriptor;
//     LogicalSource logicalSource;
// };
//
// }
//
// template <>
// struct std::hash<NES::PhysicalSource>{
//     uint64_t operator()(const NES::PhysicalSource& physicalSource) const noexcept {
//         return physicalSource.getID();
//     }
// };
//
// template <>
// struct fmt::formatter<NES::PhysicalSource> : formatter<string_view>
// {
//     format_context::iterator format(const NES::PhysicalSource& source, format_context& ctx) const;
// };