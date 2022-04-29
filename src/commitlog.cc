
#include "../include/commitlog.hh"
#pragma once

// TODO: get these values from the config.yml file
const char * DEFAULT_COMMITLOG_LOCATION = "../logs/" ;
int32_t COMMITLOG_MAX_SIZE_IN_MEGABYTES = 10;

Commitlog::Commitlog(const char *filename)  : max_size(COMMITLOG_MAX_SIZE_IN_MEGABYTES*1024/10){//TODO: change this to COMMITLOG_MAX_SIZE_IN_MEGABYTES*1024*1024;

  std::string fullpath(DEFAULT_COMMITLOG_LOCATION);
  fullpath.append(filename);
  log_file_name = fullpath.c_str();
  log_file_descriptor = open(fullpath.c_str(), O_RDWR | O_CREAT | O_EXCL , 0755);
  if ((log_file_descriptor == -1) && (EEXIST == errno)) {
    // The file is already there, instead of creating it, we'll read its header
    log_file_descriptor = open(fullpath.c_str(), O_RDWR , 0755 ) ;
    if (read_file_header())
      std::cout << "File header read successfully - Stats: archived=" << is_archived() << " active="<< is_active() << std::endl;

  } else {
    // The file is created for the first time
    append_position = 0;
    archived = false;
    active = true;
    size = Commitlog::COMMITLOG_HEADER_SIZE; //33
    version = Commitlog::CURRENT_VERSION; // 0

    if (construct_file_header())
      std::cout << "header constructed successfully"<< std::endl;
    std::cout << "File created"<< std::endl;
  }
}

Commitlog::Commitlog() : Commitlog(DEFAULT_COMMITLOG_LOCATION){};

Commitlog::~Commitlog(){
  close(log_file_descriptor);
}

void Commitlog::flush(std::unique_ptr<char []> buffer, int32_t buffer_size) {
  ssize_t bytes_written;
  lseek(log_file_descriptor,0, SEEK_END);
  size += buffer_size;
  bytes_written = write(log_file_descriptor, buffer.get(), buffer_size);
  size += bytes_written;
}

bool Commitlog::construct_file_header(){
  /**
   * Commitlog file header (33 bytes)
   * ---------------------------------------------------------------------------------------------------
   * | DIBIBASE_LOG_FILE | version  | ID       |  is_archived | is_active  | Size    | append_position |
   * | 17 bytes          | 2 bytes  | 4 bytes  |    1 byte    | 1 byte     | 4 bytes | 4 bytes         |
   * ---------------------------------------------------------------------------------------------------
   */
  std::unique_ptr<char []> buffer (new char[Commitlog::COMMITLOG_HEADER_SIZE]);
  memcpy(buffer.get(), "DIBIBASE_LOG_FILE" , 17); // used to identify commit log files from other files in the same directory
  int offset = 17;
  append_position = Commitlog::COMMITLOG_HEADER_SIZE; // append position for records, starts after buffer
  memcpy(buffer.get()+offset, reinterpret_cast<unsigned char *>(&version), sizeof(int16_t));
  offset += sizeof(int16_t);
  memcpy(buffer.get()+offset, reinterpret_cast<unsigned char *>(&id), sizeof(int32_t));
  offset += sizeof(int32_t);
  memcpy(buffer.get()+offset, reinterpret_cast<unsigned char *>(&archived), sizeof(bool));
  offset += sizeof(bool);
  memcpy(buffer.get()+offset, reinterpret_cast<unsigned char *>(&active), sizeof(bool));
  offset += sizeof(bool);
  memcpy(buffer.get()+offset, reinterpret_cast<unsigned char *>(&size), sizeof(int32_t));
  offset += sizeof(int32_t);
  memcpy(buffer.get()+offset, reinterpret_cast<unsigned char *>(&append_position), sizeof(int32_t));

  // writing the buffer to the log file
  lseek(log_file_descriptor,0, SEEK_SET);
  size_t bytes_written;
  bytes_written = write(log_file_descriptor, buffer.get(), Commitlog::COMMITLOG_HEADER_SIZE);

  if (bytes_written == Commitlog::COMMITLOG_HEADER_SIZE) {
    return true;
  }
  return false;
}

bool Commitlog::read_file_header(){
  std::unique_ptr<char []> buffer (new char[Commitlog::COMMITLOG_HEADER_SIZE+1]);
  size_t offset = 0;
  lseek(log_file_descriptor,0, SEEK_SET);
  offset = read(log_file_descriptor, buffer.get(), Commitlog::COMMITLOG_HEADER_SIZE);
  // verifying that the open file is a valid dibibase log file
  std::string data;
  for (int i = 0; i < 17; ++i) {
    data.push_back((char)buffer[i]);
  }
  if (data!="DIBIBASE_LOG_FILE"){
    // file is in the logs directory but not a log file
    return false;
  }
  // reconstructing log parameters from file header
  memcpy(reinterpret_cast<unsigned char *>(&version), buffer.get()+Commitlog::VERSION_OFFSET ,sizeof (int16_t));
  memcpy(reinterpret_cast<unsigned char *>(&id), buffer.get()+Commitlog::ID_OFFSET ,sizeof (int32_t));
  memcpy(reinterpret_cast<unsigned char *>(&archived), buffer.get()+Commitlog::ARCHIVED_STATUS_OFFSET ,sizeof(bool));
  memcpy(reinterpret_cast<unsigned char *>(&active), buffer.get()+Commitlog::ACTIVE_STATUS_OFFSET ,sizeof(bool));
  memcpy(reinterpret_cast<unsigned char *>(&size), buffer.get()+Commitlog::SIZE_OFFSET ,sizeof (int32_t));
  memcpy(reinterpret_cast<unsigned char *>(&append_position), buffer.get()+Commitlog::APPEND_POSITION_OFFSET ,sizeof (int32_t));
  return true;
}

void Commitlog::update_active_status(bool status){
  active = status;
  std::unique_ptr<char []> buffer (new char[sizeof(bool)]);
  memcpy(buffer.get(), reinterpret_cast<unsigned char *>(&active), sizeof(bool));
  lseek(log_file_descriptor, Commitlog::ACTIVE_STATUS_OFFSET, SEEK_SET);
  write(log_file_descriptor, buffer.get(), sizeof(bool));
}
void Commitlog::update_archived_status(bool status){
  archived = status;
  std::unique_ptr<char []> buffer (new char[sizeof(bool)]);
  memcpy(buffer.get(), reinterpret_cast<unsigned char *>(&archived), sizeof(bool));
  lseek(log_file_descriptor, Commitlog::ARCHIVED_STATUS_OFFSET, SEEK_SET);
  write(log_file_descriptor, buffer.get(), sizeof(bool));
}
void Commitlog::update_size(int32_t new_size){
  size = new_size;
  std::unique_ptr<char []> buffer (new char[sizeof(int32_t)]);
  memcpy(buffer.get(), reinterpret_cast<unsigned char *>(&size), sizeof(int32_t));
  lseek(log_file_descriptor, Commitlog::SIZE_OFFSET, SEEK_SET);
  write(log_file_descriptor, buffer.get(), sizeof(int32_t));
}
void Commitlog::update_append_position(int32_t offset){
  append_position = offset;
  std::unique_ptr<char []> buffer (new char[sizeof(int32_t)]);
  memcpy(buffer.get(), reinterpret_cast<unsigned char *>(&append_position), sizeof(int32_t));
  lseek(log_file_descriptor, Commitlog::APPEND_POSITION_OFFSET, SEEK_SET);
  write(log_file_descriptor, buffer.get(), sizeof(int32_t));
}


int32_t Commitlog::get_last_lsn() {
  int32_t record_size, record_lsn;
  int32_t largest_lsn = 0;

  std::unique_ptr<char []> buffer  (new char[2000]);

  int32_t read_offset = 1; // dummy value, doesn't matter unless it's 0, used to determine reaching EoF
  int32_t seek_offset = Commitlog::COMMITLOG_HEADER_SIZE;// The first position after the file header
  while (read_offset != 0 ) { // read returns 0 if we reached EOF

    // 12 bytes are read as we want to read the size and lsn of the record
    // first 3 fields of record header: size(4 bytes)|type(4 bytes)|lsn(4 bytes)|
    lseek(log_file_descriptor,seek_offset, SEEK_SET);
    read_offset = read(log_file_descriptor, buffer.get(), 12);
    if (read_offset == -1) {
      // read returned an error
      break;
      // TODO: log the errno
    }

    // getting the size of the record
    memcpy(reinterpret_cast<unsigned char *>(&record_size), buffer.get() ,sizeof (int32_t));
    // read the lsn of the record
    memcpy(reinterpret_cast<unsigned char *>(&record_lsn), buffer.get()+8 ,sizeof (int32_t));
    // compare to the current largest lsn
    if(record_lsn > largest_lsn)
      largest_lsn = record_lsn;

    seek_offset += record_size;
  }
  size = seek_offset-record_size; // this is a workaround to finding the file size, since we'll be traversing each file once on start-up anyway
  return largest_lsn;
}

