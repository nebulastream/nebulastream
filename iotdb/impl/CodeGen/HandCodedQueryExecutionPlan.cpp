/*
 * HandCodedQueryExecutionPlan.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::HandCodedQueryExecutionPlan);

#include <CodeGen/QueryExecutionPlan.hpp>

namespace iotdb {

HandCodedQueryExecutionPlan::HandCodedQueryExecutionPlan() : QueryExecutionPlan() {}

HandCodedQueryExecutionPlan::~HandCodedQueryExecutionPlan() {}
}
