#include "Util/UtilityFunctions.hpp"
#include <Experimental/Utility/TPCHUtil.hpp>
#include <filesystem>
#include <fstream>

namespace NES {

std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
TPCHUtil::getLineitems(std::string rootPath, std::shared_ptr<Runtime::BufferManager> bm, bool useCache) {

    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    // 1
    schema->addField("l_orderkey", BasicType::INT64);
    // 2
    schema->addField("l_partkey", BasicType::INT64);
    // 3
    schema->addField("l_suppkey", BasicType::INT64);
    // 4
    schema->addField("l_linenumber", BasicType::INT64);
    // 5
    schema->addField("l_quantity", BasicType::INT64);
    // 6
    schema->addField("l_extendedprice", BasicType::INT64);
    // 7
    schema->addField("l_discount", BasicType::INT64);
    // 8
    schema->addField("l_tax", BasicType::INT64);
    // 9
    schema->addField("l_returnflag", BasicType::INT64);
    // 10
    schema->addField("l_linestatus", BasicType::INT64);
    // 11
    schema->addField("l_shipdate", BasicType::INT64);
    // commitdate
    // receiptdate
    // shipinstruct
    // shipmode
    // comment

    NES_DEBUG("Loading of Lineitem done");

    if (std::filesystem::exists(rootPath + "lineitem.cache") && useCache) {
        return getFileFromCache(rootPath + "lineitem.cache", bm, schema);
    } else {
        auto result = getLineitemsFromFile(rootPath + "lineitem.tbl", bm, schema);
        if (useCache) {
            storeBuffer(rootPath + "lineitem.cache", result.second);
        }
        return result;
    }
}

std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
TPCHUtil::getFileFromCache(std::string path, std::shared_ptr<Runtime::BufferManager> bm, SchemaPtr schema) {
    NES_DEBUG("Load buffer from cache " << path);

    std::ifstream input(path, std::ios::binary | std::ios::ate);
    auto size = (unsigned) input.tellg();
    auto buf = bm->getUnpooledBuffer(size).value();
    input.seekg(0);
    input.read(buf.getBuffer<char>(), size);
    uint64_t generated_tuples_this_pass = size / schema->getSchemaSizeInBytes();
    buf.setNumberOfTuples(generated_tuples_this_pass);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buf);
    return std::make_pair(memoryLayout, dynamicBuffer);
}

void TPCHUtil::storeBuffer(std::string path, Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
    NES_DEBUG("Store buffer with " << buffer.getNumberOfTuples() << " records to " << path);
    std::ofstream outputFile;
    outputFile.open(path, std::ios::out | std::ofstream::binary);
    outputFile.write((char*) buffer.getBuffer().getBuffer(), buffer.getBuffer().getBufferSize());
    outputFile.close();
}

std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
TPCHUtil::getLineitemsFromFile(std::string path, std::shared_ptr<Runtime::BufferManager> bm, SchemaPtr schema) {
    NES_DEBUG("Load buffer from file" << path);

    std::ifstream inFile(path);
    uint64_t linecount = 0;
    std::string line;
    while (std::getline(inFile, line)) {
        // using printf() in all tests for consistency
        linecount++;
    }
    NES_DEBUG("LOAD lineitem with " << linecount << " lines");

    auto targetBufferSize = schema->getSchemaSizeInBytes() * linecount;
    auto buffer = bm->getUnpooledBuffer(targetBufferSize).value();
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);

    inFile.clear();// clear fail and eof bits
    inFile.seekg(0, std::ios::beg);

    while (std::getline(inFile, line)) {
        // using printf() in all tests for consistency
        auto index = dynamicBuffer.getNumberOfTuples();
        auto strings = NES::Util::splitWithStringDelimiter<std::string>(line, "|");

        // orderkey
        auto l_orderkeyString = strings[0];
        int64_t l_orderkey = std::stoi(l_orderkeyString);
        dynamicBuffer[index][0].write(l_orderkey);

        // partkey
        auto l_partkeyString = strings[1];
        int64_t l_partkey = std::stoi(l_partkeyString);
        dynamicBuffer[index][1].write(l_partkey);

        // suppkey
        auto l_suppkeyString = strings[2];
        int64_t l_subpkey = std::stoi(l_suppkeyString);
        dynamicBuffer[index][2].write(l_subpkey);

        // linenumber
        auto l_linenumberString = strings[3];
        int64_t l_linenumber = std::stoi(l_linenumberString);
        dynamicBuffer[index][3].write(l_linenumber);

        // quantity
        auto l_quantityString = strings[4];
        int64_t l_quantity = std::stoi(l_quantityString);
        dynamicBuffer[index][4].write(l_quantity);

        // extendedprice
        auto l_extendedpriceString = strings[5];
        int64_t l_extendedprice = std::stof(l_extendedpriceString) * 100;
        dynamicBuffer[index][5].write(l_extendedprice);

        // discount
        auto l_discountString = strings[6];
        int64_t l_discount = std::stof(l_discountString) * 100;
        dynamicBuffer[index][6].write(l_discount);

        // tax
        auto l_taxString = strings[7];
        int64_t l_tax = std::stof(l_taxString) * 100;
        dynamicBuffer[index][7].write(l_tax);

        // returnflag
        auto l_returnflagString = strings[8];
        int64_t l_returnflag = (int8_t) l_returnflagString[0];
        dynamicBuffer[index][8].write(l_returnflag);

        // returnflag
        auto l_linestatusString = strings[9];
        int64_t l_linestatus = (int8_t) l_linestatusString[0];
        dynamicBuffer[index][9].write(l_linestatus);

        auto l_shipdateString = strings[10];
        NES::Util::findAndReplaceAll(l_shipdateString, "-", "");
        int64_t l_shipdate = std::stoi(l_shipdateString);
        dynamicBuffer[index][10].write(l_shipdate);
        dynamicBuffer.setNumberOfTuples(index + 1);
    }
    inFile.close();
    return std::make_pair(memoryLayout, dynamicBuffer);
}

}// namespace NES