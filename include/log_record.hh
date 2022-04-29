
#include <memory>
#include <sstream>
#include <string>
#pragma once

#ifndef COMMITLOG_PROTOTYPE_LOG_RECORD_HH
#define COMMITLOG_PROTOTYPE_LOG_RECORD_HH

/**
 * Log Record Structure:
 * Insert:
 * -----------------------------------------------------------------------------------------
 * | Log Size | Log Type | LSN | Previous LSN | TableID  | Entry Size | Entry Data |
 * | 4 bytes  | 4 bytes  | 4   |   4 bytes    | 4 bytes  |   4 bytes  | variable   |
 * |              20 bytes- HEADER                       |          body           |
 * -----------------------------------------------------------------------------------------
 * Update:
 * -----------------------------------------------------------------------------------------
 * | Log Size | Log Type | LSN | Previous LSN | TableID | TupleID | Entry Size | Entry Data |
 * -----------------------------------------------------------------------------------------
 * Delete:
 * -----------------------------------------------------------------------------------------
 * | Log Size | Log Type | LSN | Previous LSN | TableID | TupleID |
 * -----------------------------------------------------------------------------------------
 */
enum class LogRecordType {
    INVALID = 0,
    INSERT,
    UPDATE,
    DELETE
};

class LogRecord {
public:
  // INSERT-type record constructor
  LogRecord(int32_t prev_lsn, LogRecordType log_record_type, const int32_t &tableID,
            int32_t data_size, std::string data);
  // UPDATE-type record constructor
  LogRecord(int32_t prev_lsn, LogRecordType log_record_type, const int32_t &tableID,
            int32_t tupleID, int32_t data_size, std::string data);
  // DELETE-type record constructor
  LogRecord(int32_t prev_lsn, LogRecordType log_record_type, const int32_t &tableID,
            int32_t tupleID);

  // Constructor from a buffer
  LogRecord(std::unique_ptr<char []> buffer);

  // construct header for record
  std::unique_ptr<char []> construct_record_buffer();

  // getters & setters
  inline void set_lsn(int16_t number) { lsn = number;};
  inline int32_t get_size() const { return size;};
  inline LogRecordType get_type() const { return record_type;};
  inline int32_t get_lsn() const { return lsn;};
  inline int32_t get_previous_lsn() const { return previous_lsn;};
  inline int32_t get_table() const { return table_id;};
  inline int32_t get_tuple() const { return tuple_id;};
  inline int32_t get_data_size() const { return entry_size;};
  inline std::string get_data() const{ return entry_data;};
// for testing
// std::string to_string();

  const static auto HEADER_SIZE = 20; // 4 + 4 + 4 + 4 + 4 : The first 5 fields are the same in all record types

private:
  int32_t size = 0; // length of the entire record
  LogRecordType record_type = LogRecordType::INVALID;
  int32_t  lsn = 0;
  int32_t previous_lsn = 0;
  int32_t table_id, tuple_id;
  int32_t entry_size;
  std::string entry_data;

};

#endif //COMMITLOG_PROTOTYPE_LOG_RECORD_HH
