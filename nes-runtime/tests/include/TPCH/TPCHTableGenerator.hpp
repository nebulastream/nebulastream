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

#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_TPCHTABLEGENERATOR_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_TPCHTABLEGENERATOR_HPP_

#include <API/Schema.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <TPCH/Table.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <algorithm>
#include <string>
#include <unordered_map>
extern "C" {
#include <tpch_dbgen.h>
}
extern "C" {
#include <dss.h>
#include <dsstypes.h>
#include <rnd.h>
}

namespace {
enum class TPCHTable : uint8_t { Part, PartSupp, Supplier, Customer, Orders, LineItem, Nation, Region };
const std::unordered_map<TPCHTable, std::underlying_type_t<TPCHTable>> tpch_table_to_dbgen_id = {{TPCHTable::Part, PART},
                                                                                                 {TPCHTable::PartSupp, PSUPP},
                                                                                                 {TPCHTable::Supplier, SUPP},
                                                                                                 {TPCHTable::Customer, CUST},
                                                                                                 {TPCHTable::Orders, ORDER},
                                                                                                 {TPCHTable::LineItem, LINE},
                                                                                                 {TPCHTable::Nation, NATION},
                                                                                                 {TPCHTable::Region, REGION}};

template<typename DSSType, typename MKRetType, typename... Args>
DSSType call_dbgen_mk(size_t idx, MKRetType (*mk_fn)(DSS_HUGE, DSSType* val, Args...), TPCHTable table, Args... args) {
    /**
   * Preserve calling scheme (row_start(); mk...(); row_stop(); as in dbgen's gen_tbl())
   */

    const auto dbgen_table_id = tpch_table_to_dbgen_id.at(table);

    row_start(dbgen_table_id);

    DSSType value{};
    mk_fn(idx, &value, std::forward<Args>(args)...);

    row_stop(dbgen_table_id);

    return value;
}
}// namespace
namespace NES {

enum class TPCH_Scale_Factor : uint8_t { F1, F0_1, F0_01 };

class TPCHTableGenerator {
  public:
    const std::unordered_map<TPCHTable, std::string> tpch_table_names = {{TPCHTable::Part, "part"},
                                                                         {TPCHTable::PartSupp, "partsupp"},
                                                                         {TPCHTable::Supplier, "supplier"},
                                                                         {TPCHTable::Customer, "customer"},
                                                                         {TPCHTable::Orders, "orders"},
                                                                         {TPCHTable::LineItem, "lineitem"},
                                                                         {TPCHTable::Nation, "nation"},
                                                                         {TPCHTable::Region, "region"}};

    const SchemaPtr customersSchema = Schema::create()
                                          ->addField("c_custkey", BasicType::INT32)
                                          //  ->addField("c_name", BasicType::INT64)
                                          //  ->addField("c_address", BasicType::INT64)
                                          ->addField("c_nationkey", BasicType::INT32)
                                          //  ->addField("c_phone", BasicType::INT64)
                                          ->addField("c_acctbal", BasicType::FLOAT32)
                                          ->addField("c_mksegment", BasicType::INT32);
    //->addField("c_comment", BasicType::INT64)

    const SchemaPtr ordersSchema = Schema::create()
                                       ->addField("o_orderkey", BasicType::INT32)
                                       ->addField("o_custkey", BasicType::INT32)
                                       ->addField("o_orderstatus", BasicType::INT8)
                                       ->addField("o_totalprice", BasicType::FLOAT32)
                                       ->addField("o_orderdate", BasicType::INT32)
                                       //->addField("o_orderpriority", BasicType::INT64)
                                       //->addField("o_clerk", BasicType::INT64)
                                       ->addField("o_shippriority", BasicType::INT32);
    //  ->addField("o_comment", BasicType::INT64);

    const SchemaPtr lineItemSchema = Schema::create()
                                         ->addField("l_orderkey", BasicType::INT32)
                                         ->addField("l_partkey", BasicType::INT32)
                                         ->addField("l_suppkey", BasicType::INT32)
                                         ->addField("l_lcnt", BasicType::INT32)
                                         ->addField("l_quantity", BasicType::INT32)
                                         ->addField("l_extendedprice", BasicType::FLOAT32)
                                         ->addField("l_discount", BasicType::FLOAT32)
                                         ->addField("l_tax", BasicType::FLOAT32)
                                         ->addField("l_returnflag", BasicType::INT8)
                                         ->addField("l_linestatus", BasicType::INT8)
                                         ->addField("l_shipdate", BasicType::INT32)
                                         ->addField("l_commitdate", BasicType::INT32)
                                         ->addField("l_receiptdate", BasicType::INT32);

    const SchemaPtr partSchema = Schema::create()
                                     ->addField("p_partkey", BasicType::INT32)
                                     // ->addField("p_name", BasicType::INT32)
                                     //->addField("p_mfgr", BasicType::INT32)
                                     //->addField("p_brand", BasicType::INT32)
                                     // ->addField("p_type", BasicType::FLOAT32)
                                     ->addField("p_size", BasicType::INT32)
                                     // ->addField("p_container", BasicType::FLOAT32)
                                     ->addField("p_retailprice", BasicType::FLOAT32);
    //->addField("p_comment", BasicType::INT8);

    const SchemaPtr partsuppSchema = Schema::create()
                                         ->addField("ps_partkey", BasicType::INT32)
                                         ->addField("ps_suppkey", BasicType::INT32)
                                         ->addField("ps_availqty", BasicType::INT32)
                                         ->addField("ps_supplycost", BasicType::FLOAT32);
    //->addField("ps_comment", BasicType::INT8);

    const SchemaPtr supplierSchema = Schema::create()
                                         ->addField("s_suppkey", BasicType::INT32)
                                         // ->addField("s_name", BasicType::INT32)
                                         // ->addField("s_address", BasicType::INT32)
                                         ->addField("s_nationkey", BasicType::INT32)
                                         // ->addField("s_phone", BasicType::FLOAT32)
                                         ->addField("s_acctbal", BasicType::FLOAT32);
    //->addField("s_comment", BasicType::FLOAT32);

    const SchemaPtr nationSchema = Schema::create()
                                       ->addField("n_nationkey", BasicType::INT32)
                                       ->addField("n_name", BasicType::INT32)
                                       ->addField("n_regionkey", BasicType::INT32);
    // ->addField("n_comment", BasicType::INT32);

    const SchemaPtr regionSchema = Schema::create()
                                       ->addField("r_regionkey", BasicType::INT32)
                                       ->addField("r_name", BasicType::INT32);
    //->addField("r_comment", BasicType::INT32);

    const std::unordered_map<TPCHTable, SchemaPtr> tableSchemas = {{TPCHTable::Part, partSchema},
                                                                   {TPCHTable::PartSupp, partsuppSchema},
                                                                   {TPCHTable::Supplier, supplierSchema},
                                                                   {TPCHTable::Customer, customersSchema},
                                                                   {TPCHTable::Orders, ordersSchema},
                                                                   {TPCHTable::LineItem, lineItemSchema},
                                                                   {TPCHTable::Nation, nationSchema},
                                                                   {TPCHTable::Region, regionSchema}};

    std::unordered_map<TPCHTable, NES::Runtime::MemoryLayouts::MemoryLayoutPtr> layouts;
    TPCHTableGenerator(const Runtime::BufferManagerPtr& bufferManager, TPCH_Scale_Factor scale_factor)
        : bufferManager(bufferManager), scaleFactor(scale_factor) {
        for (auto& [table, schema] : tableSchemas) {
            layouts[table] = Runtime::MemoryLayouts::ColumnLayout::create(schema, bufferManager->getBufferSize());
        }
    }

    auto getLocalScaleFactor() {
        switch (scaleFactor) {
            case TPCH_Scale_Factor::F1: return 1.0f;
            case TPCH_Scale_Factor::F0_1: return 0.1f;
            case TPCH_Scale_Factor::F0_01: return 0.01f;
        }
    }

    auto generate() {
        auto _scale_factor = getLocalScaleFactor();
        NES_ASSERT(_scale_factor < 1.0f || std::round(_scale_factor) == _scale_factor,
                   "Due to tpch_dbgen limitations, only scale factors less than one can have a fractional part.");

        const auto cache_directory =
            fmt::format("tpch_cached_tables/sf-{}-c-{}", _scale_factor, bufferManager->getBufferSize());// NOLINT
        if (std::filesystem::is_directory(cache_directory)) {
            return loadTablesFromPath(cache_directory);
        }

        // Init tpch_dbgen - it is important this is done before any data structures from tpch_dbgen are read.
        dbgen_reset_seeds();
        dbgen_init_scale_factor(_scale_factor);
        using ChunkOffset = uint64_t;
        const auto customer_count = static_cast<ChunkOffset>(tdefs[CUST].base * scale);
        const auto order_count = static_cast<ChunkOffset>(tdefs[ORDER].base * scale);
        const auto part_count = static_cast<ChunkOffset>(tdefs[PART].base * scale);
        const auto supplier_count = static_cast<ChunkOffset>(tdefs[SUPP].base * scale);
        const auto nation_count = static_cast<ChunkOffset>(tdefs[NATION].base);
        const auto region_count = static_cast<ChunkOffset>(tdefs[REGION].base);

        // The `* 4` part is defined in the TPC-H specification.
        // TableBuilder lineitem_builder{_benchmark_config->chunk_size,
        //                             lineitem_column_types,
        //                            lineitem_column_names,
        //                           ChunkOffset{order_count * 4}};

        NES_DEBUG("Generate lineitem with size {}", order_count * 4);

        Runtime::TableBuilder customerBuilder(bufferManager, layouts[TPCHTable::Customer]);
        Runtime::TableBuilder lineitemBuilder(bufferManager, layouts[TPCHTable::LineItem]);
        Runtime::TableBuilder ordersBuilder(bufferManager, layouts[TPCHTable::Orders]);
        Runtime::TableBuilder partBuilder(bufferManager, layouts[TPCHTable::Part]);
        Runtime::TableBuilder partsuppBuilder(bufferManager, layouts[TPCHTable::PartSupp]);
        Runtime::TableBuilder supplierBuilder(bufferManager, layouts[TPCHTable::Supplier]);
        Runtime::TableBuilder nationBuilder(bufferManager, layouts[TPCHTable::Nation]);
        Runtime::TableBuilder regionBuilder(bufferManager, layouts[TPCHTable::Region]);

        /**
        * CUSTOMER
        */
        for (size_t row_idx = 0; row_idx < customer_count; row_idx++) {
            auto customer = call_dbgen_mk<customer_t>(row_idx + 1, mk_cust, TPCHTable::Customer);
            // TODO fix mktsegment
            int32_t mktsegment = std::strncmp(customer.mktsegment, "BUILDING", 8) == 0;
            customerBuilder.append(std::make_tuple((int32_t) customer.custkey,
                                                   //customer.name,
                                                   //customer.address,
                                                   (int32_t) customer.nation_code,
                                                   // customer.phone,
                                                   convert_money(customer.acctbal),
                                                   (int32_t) mktsegment
                                                   //customer.comment
                                                   ));
        }

        /**
        * ORDER and LINEITEM
        */
        auto l = 0;
        for (size_t order_idx = 0; order_idx < order_count; ++order_idx) {
            const auto order = call_dbgen_mk<order_t>(order_idx + 1, mk_order, TPCHTable::Orders, 0l);
            ordersBuilder.append(std::make_tuple((int32_t) order.okey,
                                                 (int32_t) order.custkey,
                                                 /*write as one char*/ (int8_t) order.orderstatus,
                                                 convert_money(order.totalprice),
                                                 getDate(order.odate),
                                                 //order.opriority,
                                                 //order.clerk,
                                                 (int32_t) order.spriority
                                                 //order.comment
                                                 ));

            for (auto line_idx = 0; line_idx < order.lines; ++line_idx) {
                const auto& lineitem = order.l[line_idx];
                lineitemBuilder.append(std::make_tuple((int32_t) lineitem.okey,
                                                       (int32_t) lineitem.partkey,
                                                       (int32_t) lineitem.suppkey,
                                                       (int32_t) lineitem.lcnt,
                                                       (int32_t) lineitem.quantity,
                                                       convert_money(lineitem.eprice),
                                                       convert_money(lineitem.discount),
                                                       convert_money(lineitem.tax),
                                                       /*write as one char*/ (int8_t) lineitem.rflag[0],
                                                       /*write as one char*/ (int8_t) lineitem.lstatus[0],
                                                       getDate(lineitem.sdate),
                                                       getDate(lineitem.cdate),
                                                       getDate(lineitem.rdate)
                                                       //lineitem.shipinstruct,
                                                       //lineitem.shipmode,
                                                       //lineitem.comment
                                                       ));
                l++;
            }
        }

        /**
       * PART and PARTSUPP
       */
        for (size_t part_idx = 0; part_idx < part_count; ++part_idx) {
            const auto part = call_dbgen_mk<part_t>(part_idx + 1, mk_part, TPCHTable::Part);

            partBuilder.append(std::make_tuple((int32_t) part.partkey,
                                               // part.name,
                                               // part.mfgr,
                                               // part.brand,
                                               // part.type,
                                               (int32_t) part.size,
                                               // part.container,
                                               convert_money(part.retailprice)
                                               //  part.comment
                                               ));

            // Some scale factors (e.g., 0.05) are not supported by tpch-dbgen as they produce non-unique partkey/suppkey
            // combinations. The reason is probably somewhere in the magic in PART_SUPP_BRIDGE. As the partkey is
            // ascending, those are easy to identify:

            DSS_HUGE last_partkey = {};
            auto suppkeys = std::vector<DSS_HUGE>{};

            for (const auto& partsupp : part.s) {
                {
                    // Make sure we do not generate non-unique combinations (see above)
                    if (partsupp.partkey != last_partkey) {
                        NES_ASSERT(partsupp.partkey > last_partkey, "Expected partkey to be generated in ascending order");
                        last_partkey = partsupp.partkey;
                        suppkeys.clear();
                    }
                    NES_ASSERT(std::find(suppkeys.begin(), suppkeys.end(), partsupp.suppkey) == suppkeys.end(),
                               "Scale factor unsupported by tpch-dbgen. Consider choosing a \"round\" number.");
                    suppkeys.emplace_back(partsupp.suppkey);
                }

                partsuppBuilder.append(std::make_tuple((int32_t) partsupp.partkey,
                                                       (int32_t) partsupp.suppkey,
                                                       (int32_t) partsupp.qty,
                                                       convert_money(partsupp.scost)
                                                       // partsupp.comment
                                                       ));
            }
        }

        /**
       * SUPPLIER
       */
        for (size_t supplier_idx = 0; supplier_idx < supplier_count; ++supplier_idx) {
            const auto supplier = call_dbgen_mk<supplier_t>(supplier_idx + 1, mk_supp, TPCHTable::Supplier);
            supplierBuilder.append(std::make_tuple((int32_t) supplier.suppkey,
                                                   // supplier.name,
                                                   //supplier.address,
                                                   (int32_t) supplier.nation_code,
                                                   // supplier.phone,
                                                   convert_money(supplier.acctbal)
                                                   //  supplier.comment
                                                   ));
        }

        /**
       * NATION
       */
        for (size_t nation_idx = 0; nation_idx < nation_count; ++nation_idx) {
            const auto nation = call_dbgen_mk<code_t>(nation_idx + 1, mk_nation, TPCHTable::Nation);
            nationBuilder.append(std::make_tuple((int32_t) nation.code,
                                                 //nation.text,
                                                 (int32_t) nation.join
                                                 // nation.comment
                                                 ));
        }

        /**
       * REGION
       */
        for (size_t region_idx = 0; region_idx < region_count; ++region_idx) {
            const auto region = call_dbgen_mk<code_t>(region_idx + 1, mk_region, TPCHTable::Region);
            regionBuilder.append(std::make_tuple((int32_t) region.code
                                                 // region.text,
                                                 // region.comment
                                                 ));
        }

        NES_DEBUG("Generated line item with size {}", l);
        /**
        * Clean up dbgen every time we finish table generation to avoid memory leaks in dbgen
        */
        dbgen_cleanup();

        std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>> tables;
        tables[TPCHTable::LineItem] = lineitemBuilder.finishTable();
        tables[TPCHTable::Orders] = ordersBuilder.finishTable();
        tables[TPCHTable::Customer] = customerBuilder.finishTable();
        tables[TPCHTable::Nation] = nationBuilder.finishTable();
        tables[TPCHTable::Part] = partBuilder.finishTable();
        tables[TPCHTable::PartSupp] = partsuppBuilder.finishTable();
        tables[TPCHTable::Supplier] = supplierBuilder.finishTable();
        tables[TPCHTable::Region] = regionBuilder.finishTable();

        std::filesystem::create_directories(cache_directory);
        for (auto& [name, table] : tables) {
            auto tableName = std::string(magic_enum::enum_name(name));
            auto tableDir = cache_directory + "/" + tableName;
            std::filesystem::create_directories(tableDir);
            NES_INFO("Store table {} at {}", tableName, tableDir);
            Runtime::Table::store(table, tableDir);
        }
        return tables;
    }

    float convert_money(DSS_HUGE cents) {
        const auto dollars = cents / 100;
        cents %= 100;
        return static_cast<float>(dollars) + (static_cast<float>(cents)) / 100.0f;
    }

    int32_t getDate(const char x[13]) {
        auto dateString = std::string(x);
        findAndReplaceAll(dateString, "-", "");
        return std::stoi(dateString);
    }

    void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr) {
        // Get the first occurrence
        uint64_t pos = data.find(toSearch);
        // Repeat till end is reached
        while (pos != std::string::npos) {
            // Replace this occurrence of Sub String
            data.replace(pos, toSearch.size(), replaceStr);
            // Get the next occurrence from the current position
            pos = data.find(toSearch, pos + replaceStr.size());
        }
    }

    std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>> loadTablesFromPath(const std::string& cacheDirectory) {
        std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>> tables;
        for (auto table : magic_enum::enum_values<TPCHTable>()) {
            auto tableName = std::string(magic_enum::enum_name(table));
            auto tableDir = cacheDirectory + "/" + tableName;
            tables[table] = Runtime::Table::load(tableDir, bufferManager, layouts[table]);
        }
        return tables;
    }

  private:
    Runtime::BufferManagerPtr bufferManager;
    TPCH_Scale_Factor scaleFactor;
};

}// namespace NES

#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_TPCHTABLEGENERATOR_HPP_
