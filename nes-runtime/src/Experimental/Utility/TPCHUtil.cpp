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
#include <Experimental/Utility/TPCHUtil.hpp>
#include <Util/UtilityFunctions.hpp>
#include <filesystem>
#include <fstream>

namespace NES {

std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
TPCHUtil::getLineitems(std::string rootPath,
                       std::shared_ptr<Runtime::BufferManager> bm,
                       Schema::MemoryLayoutType layoutType,
                       bool useCache) {

    auto schema = Schema::create(layoutType);
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
    std::string prefix;
    if (layoutType == Schema::ROW_LAYOUT) {
        prefix = "row_";
    } else {
        prefix = "col_";
    }

    if (std::filesystem::exists(rootPath + prefix + "lineitem.cache") && useCache) {
        return getFileFromCache(rootPath + prefix + "lineitem.cache", bm, schema);
    } else {
        auto result = getLineitemsFromFile(rootPath + "lineitem.tbl", bm, schema);
        if (useCache) {
            storeBuffer(rootPath + prefix + "lineitem.cache", result.second);
        }
        return result;
    }
}

std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
TPCHUtil::getOrders(std::string rootPath,
                    std::shared_ptr<Runtime::BufferManager> bm,
                    Schema::MemoryLayoutType layoutType,
                    bool useCache) {

    auto schema = Schema::create(layoutType);
    // 1
    schema->addField("o_orderkey", BasicType::INT64);
    // 2
    schema->addField("o_custkey", BasicType::INT64);
    // 3
    schema->addField("o_orderstatus", BasicType::INT64);
    // 4
    schema->addField("o_totalprice", BasicType::INT64);
    // 5
    schema->addField("o_orderdate", BasicType::INT64);
    // 6
    schema->addField("o_clerk", BasicType::INT64);
    // 7
    schema->addField("o_shippriority", BasicType::INT64);
    // 8
    schema->addField("o_comment", BasicType::INT64);

    NES_DEBUG("Loading of Orders done");

    std::string prefix;
    if (layoutType == Schema::ROW_LAYOUT) {
        prefix = "row_";
    } else {
        prefix = "col_";
    }

    if (std::filesystem::exists(rootPath + prefix + "orders.cache") && useCache) {
        return getFileFromCache(rootPath + prefix + "orders.cache", bm, schema);
    } else {
        auto result = getOrdersFromFile(rootPath + "orders.tbl", bm, schema);
        if (useCache) {
            storeBuffer(rootPath + prefix + "orders.cache", result.second);
        }
        return result;
    }
}

std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
TPCHUtil::getCustomers(std::string rootPath,
                       std::shared_ptr<Runtime::BufferManager> bm,
                       Schema::MemoryLayoutType layoutType,
                       bool useCache) {

    auto schema = Schema::create(layoutType);
    // 1
    schema->addField("c_custkey", BasicType::INT64);
    // 2
    schema->addField("c_name", BasicType::INT64);
    // 3
    schema->addField("c_address", BasicType::INT64);
    // 4
    schema->addField("c_nationkey", BasicType::INT64);
    // 5
    schema->addField("c_phone", BasicType::INT64);
    // 6
    schema->addField("c_acctbal", BasicType::INT64);
    // 7
    schema->addField("c_mksegment", BasicType::INT64);
    // 8
    schema->addField("c_comment", BasicType::INT64);

    NES_DEBUG("Loading of customer done");

    std::string prefix;
    if (layoutType == Schema::ROW_LAYOUT) {
        prefix = "row_";
    } else {
        prefix = "col_";
    }

    if (std::filesystem::exists(rootPath + prefix + "customer.cache") && useCache) {
        return getFileFromCache(rootPath + prefix + "customer.cache", bm, schema);
    } else {
        auto result = getCustomersFromFile(rootPath + "customer.tbl", bm, schema);
        if (useCache) {
            storeBuffer(rootPath + prefix + "customer.cache", result.second);
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
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;
    if (schema->getLayoutType() == Schema::ROW_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, size);
    } else {
        memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, size);
    }
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buf);
    NES_DEBUG("Load buffer from cache done " << path);
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
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;
    if (schema->getLayoutType() == Schema::ROW_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
    } else {
        memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, buffer.getBufferSize());
    }
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

[[maybe_unused]] std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
TPCHUtil::getOrdersFromFile(std::string path, std::shared_ptr<Runtime::BufferManager> bm, SchemaPtr schema) {
    NES_DEBUG("Load buffer from file" << path);

    std::ifstream inFile(path);
    uint64_t linecount = 0;
    std::string line;
    while (std::getline(inFile, line)) {
        // using printf() in all tests for consistency
        linecount++;
    }
    NES_DEBUG("LOAD orders with " << linecount << " lines");

    auto targetBufferSize = schema->getSchemaSizeInBytes() * linecount;
    auto buffer = bm->getUnpooledBuffer(targetBufferSize).value();
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;
    if (schema->getLayoutType() == Schema::ROW_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
    } else {
        memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, buffer.getBufferSize());
    }
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);

    inFile.clear();// clear fail and eof bits
    inFile.seekg(0, std::ios::beg);

    while (std::getline(inFile, line)) {
        // using printf() in all tests for consistency
        auto index = dynamicBuffer.getNumberOfTuples();
        auto strings = NES::Util::splitWithStringDelimiter<std::string>(line, "|");

        // orderkey
        auto o_orderkeyString = strings[0];
        int64_t o_orderkey = std::stoi(o_orderkeyString);
        dynamicBuffer[index][0].write(o_orderkey);

        // custkey
        auto o_custkeyString = strings[1];
        int64_t custkey = std::stoi(o_custkeyString);
        dynamicBuffer[index][1].write(custkey);

        // orderstatus
        auto orderstatusString = strings[2];
        // int64_t l_subpkey = std::stoi(orderstatusString);
        // dynamicBuffer[index][2].write(l_subpkey);

        // totalprice
        auto o_totalpriceString = strings[3];
        //int64_t l_linenumber = std::stoi(o_totalpriceString);
        //dynamicBuffer[index][3].write(l_linenumber);

        // orderdate
        auto orderdateString = strings[4];
        NES::Util::findAndReplaceAll(orderdateString, "-", "");
        int64_t o_orderdate = std::stoi(orderdateString);
        dynamicBuffer[index][4].write(o_orderdate);

        // orderpriority
        //auto l_extendedpriceString = strings[5];
        //int64_t l_extendedprice = std::stof(l_extendedpriceString) * 100;
        // dynamicBuffer[index][5].write(l_extendedprice);

        // clerk
        //auto l_discountString = strings[6];
        //int64_t l_discount = std::stof(l_discountString) * 100;
        //dynamicBuffer[index][6].write(l_discount);

        // shippriority
        auto o_shippriorityString = strings[7];
        int64_t o_shippriority = std::stoi(o_shippriorityString);
        dynamicBuffer[index][7].write(o_shippriority);

        // comment
        //auto l_returnflagString = strings[8];
        //int64_t l_returnflag = (int8_t) l_returnflagString[0];
        //dynamicBuffer[index][8].write(l_returnflag);
        dynamicBuffer.setNumberOfTuples(index + 1);
    }
    inFile.close();
    return std::make_pair(memoryLayout, dynamicBuffer);
}

std::pair<Runtime::MemoryLayouts::MemoryLayoutPtr, Runtime::MemoryLayouts::DynamicTupleBuffer>
TPCHUtil::getCustomersFromFile(std::string path, std::shared_ptr<Runtime::BufferManager> bm, SchemaPtr schema) {
    NES_DEBUG("Load buffer from file" << path);

    std::ifstream inFile(path);
    uint64_t linecount = 0;
    std::string line;
    while (std::getline(inFile, line)) {
        // using printf() in all tests for consistency
        linecount++;
    }
    NES_DEBUG("LOAD customers with " << linecount << " lines");

    auto targetBufferSize = schema->getSchemaSizeInBytes() * linecount;
    auto buffer = bm->getUnpooledBuffer(targetBufferSize).value();
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;
    if (schema->getLayoutType() == Schema::ROW_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
    } else {
        memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, buffer.getBufferSize());
    }
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);

    inFile.clear();// clear fail and eof bits
    inFile.seekg(0, std::ios::beg);

    while (std::getline(inFile, line)) {
        // using printf() in all tests for consistency
        auto index = dynamicBuffer.getNumberOfTuples();
        auto strings = NES::Util::splitWithStringDelimiter<std::string>(line, "|");

        // custkey
        auto c_custkeyString = strings[0];
        int64_t custkey = std::stoi(c_custkeyString);
        dynamicBuffer[index][0].write(custkey);

        // name
        //auto o_custkeyString = strings[1];
        //int64_t custkey = std::stoi(o_custkeyString);
        //dynamicBuffer[index][1].write(custkey);

        // address
        // auto orderstatusString = strings[2];
        // int64_t l_subpkey = std::stoi(orderstatusString);
        // dynamicBuffer[index][2].write(l_subpkey);

        // nation
        auto c_nation = strings[3];
        //int64_t l_linenumber = std::stoi(o_totalpriceString);
        //dynamicBuffer[index][3].write(l_linenumber);

        // phone
        auto c_phone = strings[4];
        //int64_t orderdate = std::stoi(orderdate);
        //dynamicBuffer[index][4].write(l_quantity);

        // acctbal
        //auto c_acctbal = strings[5];
        //int64_t l_extendedprice = std::stof(l_extendedpriceString) * 100;
        //dynamicBuffer[index][5].write(l_extendedprice);

        // mktsegment
        auto c_mktsegmentString = strings[6];
        auto result = c_mktsegmentString.compare("BUILDING");
        if (result == 0) {
            dynamicBuffer[index][6].write((int64_t) 1);
        } else {
            dynamicBuffer[index][6].write((int64_t) 0);
        }

        // comment
        //auto c_comment = strings[7];
        //int64_t l_tax = std::stof(l_taxString) * 100;
        //dynamicBuffer[index][7].write(l_tax);
        dynamicBuffer.setNumberOfTuples(index + 1);
    }
    inFile.close();
    return std::make_pair(memoryLayout, dynamicBuffer);
}

}// namespace NES