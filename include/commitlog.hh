#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include "log_record.hh"
#include <string>

#pragma once

#ifndef COMMITLOG_PROTOTYPE_COMMITLOG_HH
#define COMMITLOG_PROTOTYPE_COMMITLOG_HH

/**
 * Commitlog file header (33 bytes)
 * -----------------------------------------------------------------------------------------------------
 * | DIBIBASE_LOG_FILE  | version  | ID       |  is_archived | is_active   | Size    | append_position |
 * | 17 bytes           | 2 bytes  | 4 bytes  |    1 byte    | 1 byte      | 4 bytes | 4 bytes         |
 * -----------------------------------------------------------------------------------------------------
 */

class Commitlog {
public:
  explicit  Commitlog();
  explicit Commitlog(const char *filename);
  ~Commitlog();

  //write a buffer to disk
  void flush(std::unique_ptr<char []> buffer, int32_t size);

  // writes the file header to the log file
  // returns true if successfully constructed
  bool construct_file_header();

  // reconstructs the log parameters from a file header, returns true if successfully constructed
  // false if the file opened was not a log file, or the header is invalid
  bool read_file_header();

  // scans the log file sequentially and returns the last lsn found in the file
  int32_t get_last_lsn();

  // getters
  inline int32_t get_max_size()  const {return max_size;};
  inline int32_t get_current_size()  const {return size;};
  inline int32_t get_file_descriptor() const {return log_file_descriptor;}
  inline int32_t get_offset_position()  const{return append_position;};
  inline bool is_archived() const {return archived;}
  inline bool is_active() const {return active;}
  inline const char *get_filename() const {return log_file_name;};

  // updaters for specific attributes in file header
  void update_active_status(bool status);
  void update_archived_status(bool status);
  void update_size(int32_t new_size);
  void update_append_position(int32_t offset);


    const static auto COMMITLOG_HEADER_SIZE = 33;
    const static auto CURRENT_VERSION = 0;

    // offsets in the file header, make sure to update in case of changing the header structure
    const static auto VERSION_OFFSET = 17;
    const static auto ID_OFFSET = 19;
    const static auto ARCHIVED_STATUS_OFFSET = 23;
    const static auto ACTIVE_STATUS_OFFSET = 24;
    const static auto SIZE_OFFSET = 25;
    const static auto APPEND_POSITION_OFFSET = 29;

private:
    int log_file_descriptor;
    bool archived;
    bool active;
    int32_t max_size, size, append_position, id;
    int16_t version;
    // used to delete the file
    const char * log_file_name;
};


#endif //COMMITLOG_PROTOTYPE_COMMITLOG_HH
