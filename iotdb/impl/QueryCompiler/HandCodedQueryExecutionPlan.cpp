/*
 * HandCodedQueryExecutionPlan.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */
#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::HandCodedQueryExecutionPlan);

#include <QueryCompiler/QueryExecutionPlan.hpp>

namespace NES {

HandCodedQueryExecutionPlan::HandCodedQueryExecutionPlan() : QueryExecutionPlan("") {}

HandCodedQueryExecutionPlan::~HandCodedQueryExecutionPlan() {}
}// namespace NES
