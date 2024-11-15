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
#include <Execution/Operators/MEOS/Meos.hpp>
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

extern "C" {
    #include <stdio.h>
    #include <stdlib.h>
    //#include <libpq-fe.h>
}

using namespace std;
using namespace MEOS;

/* Number of instants in a batch for printing a marker */
#define NO_INSTANTS_BATCH 1000
/* Maximum length in characters of a header record in the input CSV file */
#define MAX_LENGTH_HEADER 1024
/* Maximum length for a SQL output query */
#define MAX_LENGTH_SQL 16384
/* Number of inserts that are sent in bulk */
#define NO_BULK_INSERT 20

typedef struct {
    Timestamp T;
    long int MMSI;
    double Latitude;
    double Longitude;
    double SOG;
} AIS_record;

static void exit_nicely(PGconn* conn) {
    PQfinish(conn);
    exit(1);
}

static void exec_sql(PGconn* conn, const char* sql, ExecStatusType status) {
    PGresult* res = PQexec(conn, sql);
    if (PQresultStatus(res) != status) {
        fprintf(stderr, "SQL command failed:\n%s %s", sql, PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
}

namespace NES::Runtime::Execution {
/**
 * @brief Test Executing MEOS from the pluginm reading from CSV, connecting with Postgres, 
 * storing and analysing trajectories.
 */
class Meos04 : public ::testing::Test {
  protected:
    MEOS::Meos* meos;

    Meos04() : meos(nullptr) {}

    void SetUp() override {
        //meos = new MEOS::Meos();
        const char* conninfo;
        PGconn* conn;
        AIS_record rec;
        int no_records = 0;
        int no_nulls = 0;
        char text_buffer[MAX_LENGTH_HEADER];
        /* Maximum length in characters of the string for the bulk insert */
        char insert_buffer[MAX_LENGTH_SQL];

        /***************************************************************************
            * Section 1: Connexion to the database
            ***************************************************************************/

        /*
            * If the user supplies a parameter on the command line, use it as the
            * conninfo string; otherwise default to setting dbname=postgres and using
            * environment variables or defaults for all other connection parameters.
            */

        conninfo = "host=localhost user=postgres dbname=postgres";

        /* Make a connection to the database */
        conn = PQconnectdb(conninfo);

        /* Check to see that the backend connection was successfully made */
        if (PQstatus(conn) != CONNECTION_OK) {
            fprintf(stderr, "%s", PQerrorMessage(conn));
            exit_nicely(conn);
        }

        /* Set always-secure search path, so malicious users can't take control. */
        exec_sql(conn, "SELECT pg_catalog.set_config('search_path', '', false)", PGRES_TUPLES_OK);

        FILE* file = fopen("../../../../../../nes-plugins/meos/tests/testData/aisinput.csv", "r");
        /***************************************************************************
            * Section 3: Read input file line by line and save each observation as a
            * temporal point in MobilityDB
            ***************************************************************************/

        /* Create the table that will hold the data */
        printf("Creating the table in the database\n");
        exec_sql(conn, "DROP TABLE IF EXISTS public.AISInstants;", PGRES_COMMAND_OK);
        exec_sql(conn,
                 "CREATE TABLE public.AISInstants(MMSI integer, "
                 "location public.tgeogpoint, SOG public.tfloat);",
                 PGRES_COMMAND_OK);

        /* Start a transaction block */
        exec_sql(conn, "BEGIN", PGRES_COMMAND_OK);

        /* Read the first line of the file with the headers */
        fscanf(file, "%1023s\n", text_buffer);

        printf("Reading the instants (one marker every %d instants)\n", NO_INSTANTS_BATCH);

        /* Continue reading the file */
        int len;
        do {
            int read = fscanf(file, "%32[^,],%ld,%lf,%lf,%lf\n", text_buffer, &rec.MMSI, &rec.Latitude, &rec.Longitude, &rec.SOG);
            /* Transform the string representing the timestamp into a timestamp value */
            rec.T = pg_timestamp_in(text_buffer, -1);

            if (read == 5) {
                no_records++;
                if (no_records % NO_INSTANTS_BATCH == 0) {
                    printf("*");
                    fflush(stdout);
                }
            }

            if (read != 5 && !feof(file)) {
                printf("Record with missing values ignored\n");
                no_nulls++;
            }

            if (ferror(file)) {
                printf("Error reading file\n");
                fclose(file);
                exit_nicely(conn);
            }

            /* Create the INSERT command with the values read */
            if ((no_records - 1) % NO_BULK_INSERT == 0)
                len = sprintf(insert_buffer, "INSERT INTO public.AISInstants(MMSI, location, SOG) VALUES ");

            char* t_out = pg_timestamp_out(rec.T);
            len += sprintf(insert_buffer + len,
                           "(%ld, 'SRID=4326;Point(%lf %lf)@%s+00', '%lf@%s+00'),",
                           rec.MMSI,
                           rec.Longitude,
                           rec.Latitude,
                           t_out,
                           rec.SOG,
                           t_out);
            free(t_out);

            if ((no_records - 1) % NO_BULK_INSERT == NO_BULK_INSERT - 1) {
                /* Replace the last comma with a semicolon */
                insert_buffer[len - 1] = ';';
                exec_sql(conn, insert_buffer, PGRES_COMMAND_OK);
                len = 0;
            }
        } while (!feof(file));

        if (len > 0) {
            /* Replace the last comma with a semicolon */
            insert_buffer[len - 1] = ';';
            exec_sql(conn, insert_buffer, PGRES_COMMAND_OK);
        }

        printf("\n%d records read.\n%d incomplete records ignored.\n", no_records, no_nulls);

        sprintf(text_buffer, "SELECT COUNT(*) FROM public.AISInstants;");
        PGresult* res = PQexec(conn, text_buffer);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "SQL command failed:\n%s %s", text_buffer, PQerrorMessage(conn));
            PQclear(res);
            exit_nicely(conn);
        }

        printf("Query 'SELECT COUNT(*) FROM public.AISInstants' returned %s\n", PQgetvalue(res, 0, 0));

        /* End the transaction */
        exec_sql(conn, "END", PGRES_COMMAND_OK);
        /* Close the file */
        fclose(file);
        PQfinish(conn);
    }

    void TearDown() override {
        delete meos;
        meos = nullptr;
    }
};

TEST_F(Meos04, Meos04Test) { ASSERT_NE(meos, nullptr); }

}// namespace NES::Runtime::Execution
