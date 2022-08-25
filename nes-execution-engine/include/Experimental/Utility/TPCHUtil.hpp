#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_UTILITY_TPCHUTIL_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_UTILITY_TPCHUTIL_HPP_
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
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_UTILITY_TPCHUTIL_HPP_
