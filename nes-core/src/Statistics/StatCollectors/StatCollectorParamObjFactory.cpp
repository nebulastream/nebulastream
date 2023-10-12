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

#include <Statistics/StatCollectors/StatCollectorParamObjFactory.hpp>
#include <Statistics/StatCollectors/Synopses/Sketches/CountMin/CountMinParamObjects/CountMinBuildParamObj.hpp>
#include <Statistics/StatCollectors/Synopses/Sketches/CountMin/CountMinParamObjects/CountMinProbeParamObj.hpp>
#include <Statistics/StatCollectors/Synopses/Sketches/CountMin/CountMinParamObjects/CountMinDeleteParamObj.hpp>
#include <Statistics/StatCollectors/Synopses/Sketches/HyperLogLog/HyperLogLogParamObjects/HyperLogLogBuildParamObj.hpp>
#include <Statistics/StatCollectors/Synopses/Sketches/HyperLogLog/HyperLogLogParamObjects/HyperLogLogProbeParamObj.hpp>
#include <Statistics/StatCollectors/Synopses/Sketches/HyperLogLog/HyperLogLogParamObjects/HyperLogLogDeleteParamObj.hpp>
#include <Statistics/StatCollectors/Synopses/Sketches/DDSketch/DDSketchParamObjects/DDSketchBuildParamObj.hpp>
#include <Statistics/StatCollectors/Synopses/Sketches/DDSketch/DDSketchParamObjects/DDSketchProbeParamObj.hpp>
#include <Statistics/StatCollectors/Synopses/Sketches/DDSketch/DDSketchParamObjects/DDSketchDeleteParamObj.hpp>
#include <Statistics/StatCollectors/Synopses/Samples/ReservoirSample/ReservoirSampleParamObjects/ReservoirSampleBuildParamObj.hpp>
#include <Statistics/StatCollectors/Synopses/Samples/ReservoirSample/ReservoirSampleParamObjects/ReservoirSampleProbeParamObj.hpp>
#include <Statistics/StatCollectors/Synopses/Samples/ReservoirSample/ReservoirSampleParamObjects/ReservoirSampleDeleteParamObj.hpp>

namespace NES {

	namespace Experimental::Statistics {

		statCollectorBuildParamObjPtr StatCollectorParamObjFactory::createCountMinBuildParamObj(std::string &logicalSourceName,
		                                                                                   std::string &physicalSourceName,
		                                                                                   std::string &fieldName) {
			return std::make_shared<CountMinBuildParamObj>(logicalSourceName, physicalSourceName, fieldName);
		}

		StatCollectorProbeParamObjPtr createCountMinProbeParamObj(std::string& logicalSourceName,
		                                                          std::string& physicalSourceName,
		                                                          std::string& fieldName,
		                                                          std::string& statCollectorType,
		                                                          std::string& expression) {
			return std::make_shared<CountMinProbeParamObj>(logicalSourceName, physicalSourceName, fieldName, statCollectorType, expression);
		}

		StatCollectorDeleteParamObjPtr createCountMinDeleteParamObj(std::string& logicalSourceName,
																																std::string& physicalSourceName,
																																std::string& fieldName,
																																std::string& statCollectorType) {
			return std::make_shared<CountMinDeleteParamObj>(logicalSourceName, physicalSourceName, fieldName, statCollectorType);
		}

		statCollectorBuildParamObjPtr createHyperLogLogBuildParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName) {
			return std::make_shared<HyperLogLogBuildParamObj>(logicalSourceName, physicalSourceName, fieldName);
		}

		StatCollectorProbeParamObjPtr createHyperLogLogProbeParamObj(std::string& logicalSourceName,
		                                                             std::string& physicalSourceName,
		                                                             std::string& fieldName,
		                                                             std::string& statCollectorType,
		                                                             std::string& expression) {
			return std::make_shared<HyperLogLogProbeParamObj>(logicalSourceName, physicalSourceName, fieldName, statCollectorType, expression);
		}

		StatCollectorDeleteParamObjPtr createHyperLogLogDeleteParamObj(std::string& logicalSourceName,
																																	 std::string& physicalSourceName,
																																	 std::string& fieldName,
																																	 std::string& statCollectorType) {
			return std::make_shared<HyperLogLogDeleteParamObj>(logicalSourceName, physicalSourceName, fieldName, statCollectorType);
		}

		statCollectorBuildParamObjPtr createDDSketchBuildParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName) {
			return std::make_shared<DDSketchBuildParamObj>(logicalSourceName, physicalSourceName, fieldName);
		}
 
		StatCollectorProbeParamObjPtr createDDSketchProbeParamObj(std::string& logicalSourceName,
		                                                          std::string& physicalSourceName,
		                                                          std::string& fieldName,
		                                                          std::string& statCollectorType,
		                                                          std::string& expression) {
			return std::make_shared<DDSketchProbeParamObj>(logicalSourceName, physicalSourceName, fieldName, statCollectorType, expression);
		}

		StatCollectorDeleteParamObjPtr createDDSketchDeleteParamObj(std::string& logicalSourceName,
																																std::string& physicalSourceName,
																																std::string& fieldName,
																																std::string& statCollectorType) {
			return std::make_shared<DDSketchDeleteParamObj>(logicalSourceName, physicalSourceName, fieldName, statCollectorType);
		}

		statCollectorBuildParamObjPtr createReservoirSampleBuildParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName) {
			return std::make_shared<ReservoirSampleBuildParamObj>(logicalSourceName, physicalSourceName, fieldName);
		}

		StatCollectorProbeParamObjPtr createReservoirSampleProbeParamObj(std::string& logicalSourceName,
		                                                                 std::string& physicalSourceName,
		                                                                 std::string& fieldName,
		                                                                 std::string& statCollectorType,
		                                                                 std::string& expression) {
			return std::make_shared<ReservoirSampleProbeParamObj>(logicalSourceName, physicalSourceName, fieldName, statCollectorType, expression);
		}

		StatCollectorDeleteParamObjPtr createReservoirSampleDeleteParamObj(std::string& logicalSourceName,
																																			 std::string& physicalSourceName,
																																			 std::string& fieldName,
																																			 std::string& statCollectorType) {
			return std::make_shared<ReservoirSampleDeleteParamObj>(logicalSourceName, physicalSourceName, fieldName, statCollectorType);
		}

	}

}
