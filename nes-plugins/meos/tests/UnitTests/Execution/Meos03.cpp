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
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <Execution/Operators/MEOS/Meos.hpp>
#include <fstream>

using namespace std;


/* Maximum number of instants in an input trip */
#define MAX_INSTANTS 50000
/* Number of instants in a batch for printing a marker */
#define NO_INSTANTS_BATCH 1000
/* Maximum length in characters of a header record in the input CSV file */
#define MAX_LENGTH_HEADER 1024
/* Maximum length in characters of a point in the input data */
#define MAX_LENGTH_POINT 64
/* Maximum length in characters of a timestamp in the input data */
#define MAX_LENGTH_TIMESTAMP 32
/* Maximum number of trips */
#define MAX_TRIPS 5

typedef struct
{
  Timestamp T;
  long int MMSI;
  double Latitude;
  double Longitude;
  double SOG;
} AIS_record;

typedef struct
{
  long int MMSI;   /* Identifier of the trip */
  int numinstants; /* Number of input instants */
  TInstant *trip_instants[MAX_INSTANTS]; /* Array of instants for the trip */
  TInstant *SOG_instants[MAX_INSTANTS];  /* Array of instants for the SOG */
  Temporal *trip;  /* Trip constructed from the input instants */
  Temporal *SOG;   /* SOG constructed from the input instants */
} trip_record;

extern "C" {
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
}



namespace NES::Runtime::Execution {

class Meos03 : public ::testing::Test {
  protected:
    Meos* meos;

    Meos03() : meos(nullptr) {}

        void SetUp() override {
            meos = new Meos();
            FILE *file = fopen("/home/mariana/NebulaMeos/nes-plugins/meos/tests/testData/aisinput.csv", "r");
            AIS_record rec;
            int no_records = 0;
            int no_nulls = 0;
            char header_buffer[MAX_LENGTH_HEADER];
            char point_buffer[MAX_LENGTH_POINT];
            char timestamp_buffer[MAX_LENGTH_TIMESTAMP];

            if (file == NULL)
            {
                printf("Error opening input file\n");
                return ;
            }

            printf("Reading input file...\n");
            fscanf(file, "%1023s\n", header_buffer);

            /* Continue reading the file */
            do
            {
                int read = fscanf(file, "%31[^,],%ld,%lf,%lf,%lf\n",
                timestamp_buffer, &rec.MMSI, &rec.Latitude, &rec.Longitude, &rec.SOG);
                /* Transform the string representing the timestamp into a timestamp value */
                rec.T = pg_timestamp_in(timestamp_buffer, -1);

                if (read == 5)
                no_records++;

                if (read != 5 && !feof(file))
                {
                printf("Record with missing values ignored\n");
                no_nulls++;
                }

                if (ferror(file))
                {
                printf("Error reading input file\n");
                fclose(file);
                return ;
                }

                /* Print only 1 out of 1000 records */
                if (no_records % 1000 == 0)
                {
                char *t_out = pg_timestamp_out(rec.T);
                /* See above the assumptions made wrt the input data in the file */
                sprintf(point_buffer, "SRID=4326;Point(%lf %lf)@%s+00", rec.Longitude,
                    rec.Latitude, t_out);
                Temporal *inst1 = tgeogpoint_in(point_buffer);
                char *inst1_out = tpoint_as_text(inst1, 2);

                TInstant *inst2 = tfloatinst_make(rec.SOG, rec.T);
                char *inst2_out = tfloat_out((Temporal *) inst2, 2);
                printf("MMSI: %ld, Location: %s SOG : %s\n",
                    rec.MMSI, inst1_out, inst2_out);

                free(inst1); free(t_out); free(inst1_out);
                free(inst2); free(inst2_out);
                }

            } while (!feof(file));

            printf("\n%d no_records read.\n%d incomplete records ignored.\n",
                no_records, no_nulls);
                    

            fclose(file);        
        }

        void TearDown() override {
            delete meos;
            meos = nullptr;
        }
};

TEST_F(Meos03, Meos03Test) {
    ASSERT_NE(meos, nullptr);
}

} // namespace NES::Runtime::Execution