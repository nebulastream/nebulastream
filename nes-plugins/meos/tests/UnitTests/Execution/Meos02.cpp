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

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/meos/Meos.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <sstream>
#include <string>

using namespace std;
/* Maximum length in characters of a header record in the input CSV file */
#define MAX_LENGTH_HEADER 1024
/* Maximum length in characters of a point in the input data */
#define MAX_LENGTH_POINT 64
/* Maximum length in characters of a timestamp in the input data */
#define MAX_LENGTH_TIMESTAMP 32

using namespace std;
using namespace meos;

typedef struct {
    Timestamp T;
    long int MMSI;
    double Latitude;
    double Longitude;
    double SOG;
} AIS_record;

namespace NES::Runtime::Execution {

/**
 * @brief Test Executing meos, reading from a CSV file and creating trajectories
 */
class Meos02 : public ::testing::Test {
  protected:
    meos::Meos* meos;

    Meos02() : meos(nullptr) {}

    void SetUp() override {
        meos = new meos::Meos("UTC");
        std::fstream file("../../../../../../nes-plugins/meos/tests/testData/ais_instants.csv");
        AIS_record rec;
        int no_records = 0;
        int no_nulls = 0;
        char header_buffer[MAX_LENGTH_HEADER];
        char point_buffer[MAX_LENGTH_POINT];
        string timestamp_buffer;
        string line;

        cout << "Reading input file..." << endl;

        std::string first_line;
        std::getline(file, first_line);
        std::strncpy(header_buffer, first_line.c_str(), MAX_LENGTH_HEADER);
        header_buffer[MAX_LENGTH_HEADER - 1] = '\0';// ensure null termination

        /* Continue reading the file */
        while (getline(file, line)) {
            stringstream ss(line);
            getline(ss, timestamp_buffer, ',');
            ss >> rec.MMSI;
            ss.ignore(1, ',');

            ss >> rec.Latitude;
            ss.ignore(1, ',');

            ss >> rec.Longitude;
            ss.ignore(1, ',');

            ss >> rec.SOG;
            /* Transform the string representing the timestamp into a timestamp value */
            rec.T = pg_timestamp_in(timestamp_buffer.c_str(), -1);
            no_records++;

            /* Print only 1 out of 1000 records */
            if (no_records % 1000 == 0) {
                char* t_out = pg_timestamp_out(rec.T);
                /* See above the assumptions made wrt the input data in the file */
                sprintf(point_buffer, "SRID=4326;Point(%lf %lf)@%s+00", rec.Longitude, rec.Latitude, t_out);
                Temporal* inst1 = tgeogpoint_in(point_buffer);
                char* inst1_out = tpoint_as_text(inst1, 2);

                TInstant* inst2 = tfloatinst_make(rec.SOG, rec.T);
                char* inst2_out = tfloat_out((Temporal*) inst2, 2);
                std::cout << "MMSI: " << rec.MMSI << ", Location: " << inst1_out << " SOG : " << inst2_out << std::endl;
                free(inst1);
                free(t_out);
                free(inst1_out);
                free(inst2);
                free(inst2_out);
            }
        }

        printf("\n%d no_records read.\n%d incomplete records ignored.\n", no_records, no_nulls);

        file.close();
    }

    void TearDown() override {
        delete meos;
        meos = nullptr;
    }
};

TEST_F(Meos02, Meos02Test) { ASSERT_NE(meos, nullptr); }

}// namespace NES::Runtime::Execution
