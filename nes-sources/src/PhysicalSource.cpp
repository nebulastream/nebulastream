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
#include <Sources/PhysicalSource.hpp>

// namespace NES
// {
//
// PhysicalSource::PhysicalSource(
//     const uint64_t id, const Sources::SourceDescriptor& sourceDescriptor, const WorkerId workerId, const LogicalSource& logicalSource, )
//     : id(id)
//     , workerID(workerId)
//     , sourceDescriptor(std::make_shared<Sources::SourceDescriptor>(sourceDescriptor))
//     , logicalSource(logicalSource)
// {
// }
//
// std::shared_ptr<const Sources::SourceDescriptor> PhysicalSource::getSourceDescriptor() const
// {
//     return sourceDescriptor;
// }
// uint64_t PhysicalSource::getID() const
// {
//     return id;
// }
//
// // std::shared_ptr<const Sources::SourceDescriptor> PhysicalSource::createSourceDescriptor(Schema schema)
// // {
// //     auto copyOfConfig = sourceDescriptor.config;
// //     return std::make_unique<Sources::SourceDescriptor>(
// //         std::move(schema),
// //         sourceDescriptor.logicalSourceName,
// //         sourceDescriptor.sourceType,
// //         sourceDescriptor.parserConfig,
// //         std::move(copyOfConfig));
// // }
// }
//
// fmt::context::iterator fmt::formatter<NES::PhysicalSource, char, void>::format(const NES::PhysicalSource& source, format_context& ctx) const
// {
//     return format_to(ctx.out(), "ID: {}, Source Type: {}", source.getID(), source.getSourceDescriptor()->getSourceType());
// }
