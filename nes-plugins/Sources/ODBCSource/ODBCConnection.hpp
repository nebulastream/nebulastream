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

#pragma once

#include <chrono>
#include <cstdint>
#include <cstddef>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>
#include <sql.h>
#include <sqltypes.h>

#include <Runtime/AbstractBufferProvider.hpp>
#include <DataTypes/DataType.hpp>
#include <Runtime/TupleBuffer.hpp>


struct TypeInfo
{
    SQLSMALLINT sqlType;
    NES::DataType::Type nesType;
    size_t nesTypeSize;
    size_t sqlColumnSize;
};

namespace NES
{
enum class ODBCPollStatus : uint8_t
{
    NO_NEW_ROWS,
    FEWER_ROWS,
    NEW_ROWS,
};

class ODBCConnection
{
    struct FetchedSchema
    {
        size_t numColumns{0};
        size_t sizeOfRow{0};
        std::vector<TypeInfo> columnTypes;
    };

public:
    ODBCConnection();
    ~ODBCConnection();

    size_t getFetchedSizeOfRow() const { return fetchedSchema.sizeOfRow; }

    void fetchColumns(std::string_view connectionString);

    size_t syncRowCount();

    void connect(const std::string& connectionString, std::string_view syncTable, std::string_view query);

    template <typename T>
    SQLRETURN readVal(const size_t colIdx, NES::TupleBuffer& buffer, const TypeInfo& typeInfo, SQLLEN& indicator) const
    {
        T* val = reinterpret_cast<T*>(&buffer.getAvailableMemoryArea<char>()[buffer.getNumberOfTuples()]);
        buffer.setNumberOfTuples(buffer.getNumberOfTuples() + typeInfo.nesTypeSize);
        return SQLGetData(hstmt, colIdx, typeInfo.sqlType, val, typeInfo.nesTypeSize, &indicator);
    }

    SQLRETURN
    readVarSized(size_t colIdx, const TypeInfo& typeInfo, SQLLEN& indicator, TupleBuffer& buffer, AbstractBufferProvider& bufferProvider) const;

    // Todo: we could leave all of the 'parsing/formatting' work to 'nes-input-formatters'
    //       IF: we allowed the source to add metadata (e.g., to the buffer) that tells the input formatter, for a specific buffer,
    //           where the first tuple starts and the last tuple ends.
    //       IF: there is some way to access the binary data directly
    //       IF: there is some way to handle binary column data in the input formatter
    SQLRETURN readDataIntoBuffer(
        size_t colIdx, const TypeInfo& typeInfo, SQLLEN& indicator, TupleBuffer& buffer, AbstractBufferProvider& bufferProvider) const;

    std::vector<SQLCHAR> buildNewRowFetchSting(std::string_view userQuery, uint64_t numRowsToFetch);

    ODBCPollStatus
    executeQuery(std::string_view query, TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, size_t rowsPerBuffer);

private:
    SQLHENV henv = SQL_NULL_HENV;
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLHSTMT hstmt = SQL_NULL_HSTMT;
    SQLHSTMT hstmtCount = SQL_NULL_HSTMT;
    FetchedSchema fetchedSchema;
    uint64_t rowCountTracker{0};
    std::string countQuery;
};
}