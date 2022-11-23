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
#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_UTILITY_TPCHUTIL_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_UTILITY_TPCHUTIL_HPP_
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
namespace NES {

class TPCHUtil {
  public:
    static std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
    getLineitems(std::string path,
                 std::shared_ptr<Runtime::BufferManager> bm,
                 Schema::MemoryLayoutType layoutType,
                 bool useCache = false);

    static std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
    getLineitemsFromFile(std::string path, std::shared_ptr<Runtime::BufferManager> bm, SchemaPtr schema);

    static std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
    getFileFromCache(std::string path, std::shared_ptr<Runtime::BufferManager> bm, SchemaPtr schema);

    static void storeBuffer(std::string path, Runtime::MemoryLayouts::DynamicTupleBuffer& buffer);
    static std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
    getOrders(std::string rootPath,
              std::shared_ptr<Runtime::BufferManager> bm,
              Schema::MemoryLayoutType layoutType,
              bool useCache);
    static std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
    getOrdersFromFile(std::string path, std::shared_ptr<Runtime::BufferManager> bm, SchemaPtr schema);
    static std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
    getCustomersFromFile(std::string path, std::shared_ptr<Runtime::BufferManager> bm, SchemaPtr schema);
    static std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
    getCustomers(std::string rootPath,
                 std::shared_ptr<Runtime::BufferManager> bm,
                 Schema::MemoryLayoutType layoutType,
                 bool useCache);
};

}// namespace NES
#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_UTILITY_TPCHUTIL_HPP_
