/*
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <CodeGen/GeneratedQueryExecutionPlan.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::GeneratedQueryExecutionPlan);

#include <CodeGen/QueryExecutionPlan.hpp>
#include <API/InputQuery.hpp>

namespace iotdb {
    GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan():query(), ptr() {

    }
    GeneratedQueryExecutionPlan::GeneratedQueryExecutionPlan(InputQuery* query, PipelineStagePtr* ptr)
            : query(query), ptr(ptr) {
    }

} // namespace iotdb
