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


#ifndef NES_DATAGENERATOR_HPP
#define NES_DATAGENERATOR_HPP

#include <Runtime/BufferManager.hpp>

namespace NES::DataGeneration {
    class DataGenerator {
      public:
        DataGenerator(Runtime::BufferManagerPtr bufferManager);
        virtual ~DataGenerator() = default;

        /**
         * @brief creates the data that will be used by a DataProvider
         * @param numberOfBuffers
         * @param bufferSize
         * @return
         */
        virtual std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) = 0;

        /**
         * @brief returns the schema that belongs to this data generation
         * @return schema
         */
        virtual SchemaPtr getSchema() = 0;

        /**
         * @brief returns the name of the data generator
         * @return name of the data generator
         */
        virtual std::string getName() = 0;

        /**
         * @brief creates a data generator depending on the name
         * @param name
         * @return
         */
        static std::shared_ptr<DataGenerator> createGeneratorByName(std::string name,
                                                                    Runtime::BufferManagerPtr bufferManager);



      protected:
        /**
         * @brief allocates a buffer from the bufferManager
         * @return TupleBuffer
         */
        Runtime::TupleBuffer allocateBuffer();

        /**
         * @brief
         * @param bufferSize
         * @return
         */
        Runtime::MemoryLayouts::MemoryLayoutPtr getMemoryLayout(size_t bufferSize);

      private:
        Runtime::BufferManagerPtr bufferManager;
    };
}

#endif//NES_DATAGENERATOR_HPP
