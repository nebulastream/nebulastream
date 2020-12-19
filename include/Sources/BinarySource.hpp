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

#ifndef INCLUDE_BINARYSOURCE_H_
#define INCLUDE_BINARYSOURCE_H_

#include <Sources/DataSource.hpp>
#include <fstream>

namespace NES {

/**
 * @brief this class provides a binary file as source
 */
class BinarySource : public DataSource {
  public:
    /**
     * @brief constructor for binary source
     * @param schema of the data source
     * @param file path
     */
    explicit BinarySource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                          const std::string& file_path, OperatorId operatorId);

    /**
     * @brief override the receiveData method for the binary source
     * @return returns a buffer if available
     */
    std::optional<NodeEngine::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the binary source
     * @return returns string describing the binary source
     */
    const std::string toString() const override;

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    void fillBuffer(NodeEngine::TupleBuffer&);

    SourceType getType() const override;

    const std::string& getFilePath() const;

  private:
    std::ifstream input;
    std::string file_path;

    int file_size;
    uint64_t tuple_size;
};

typedef std::shared_ptr<BinarySource> BinarySourcePtr;

}// namespace NES
#endif /* INCLUDE_BINARYSOURCE_H_ */
