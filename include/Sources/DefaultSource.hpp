/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef INCLUDE_SOURCESINK_DEFAULTSOURCE_HPP_
#define INCLUDE_SOURCESINK_DEFAULTSOURCE_HPP_
#include <Sources/DataSource.hpp>
#include <Sources/GeneratorSource.hpp>

namespace NES {

class DefaultSource : public GeneratorSource {
  public:
    DefaultSource() = default;
    DefaultSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                  const uint64_t numbersOfBufferToProduce, uint64_t frequency, OperatorId operatorId);

    SourceType getType() const override;

    std::optional<TupleBuffer> receiveData() override;
};

typedef std::shared_ptr<DefaultSource> DefaultSourcePtr;

}// namespace NES
#endif /* INCLUDE_SOURCESINK_DEFAULTSOURCE_HPP_ */
