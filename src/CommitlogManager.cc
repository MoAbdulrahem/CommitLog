#include "CommitlogManager.hh"

#pragma once

CommitlogManager::CommitlogManager(SyncMode mode)
  : sync_mode{mode}, commitlog(nullptr), default_directory("../" + DEFAULT_COMMITLOG_DIRECTORY + "/"),
  need_flush(false) {

  lsn.store(0);
  // scanning all the files in the given directory TODO: get the directory from the yml config file
  DIR *d;
  struct dirent *dir;
  d = opendir(default_directory.c_str());
  if (d) {
    while ((dir = readdir(d)) != nullptr) {
      if (dir->d_type == DT_REG) { // filtering the regular files from directories or system/hidden files
        std::string file = std::string(dir->d_name);

        if (file.substr(0,18)== "dibibase_log_file_"){ // checking that a file has Dibibase log files naming conventions
          std::unique_ptr<Commitlog> temp_log = std::make_unique<Commitlog>(file.c_str());

          // whether the file is archived or not, we'd need to know the last lsn it holds
          int32_t file_largest_lsn = temp_log->get_last_lsn();
          if (file_largest_lsn > last_lsn){
            last_lsn = file_largest_lsn;
            std::cout << "last lsn updated with value " << last_lsn << std::endl;
          }
          // We append to a file if it's active and Not archived
          if (!temp_log->is_archived() && temp_log->is_active() ){
              commitlog = move(temp_log);

            } else if (!temp_log->is_archived() && !temp_log->is_active() ){
              // file is not active and not-archived (we can't append to itn but it's still not persistent in DB so we have to keep it
              logs_to_flush.push(move(temp_log));
            } else {
            // file is marked as archived, It was written to the DB and is no longer needed
            logs_to_delete.push(move(temp_log));
            //TODO: handle it (delete it? need a way to make sure the DB no longer needs it)
          }
        }
    }
    }
    closedir(d);
  }

  lsn.store(++last_lsn); // value would be 1 in case of no files found
  if (commitlog == nullptr) { // No active log files were found, so we create a new one
    commitlog = create_new_log_file();
  }
}

CommitlogManager::~CommitlogManager() {
  if (log_buffer_offset != 0) {
    // log buffer is not empty, so we need to flush it
    force_flush();
  }
 }

int32_t CommitlogManager::append_log_record(LogRecord &record) {

  std::unique_lock<std::mutex> latch(mutex_latch);
  record.set_lsn(lsn);
  // read the header of the record
  std::unique_ptr<char []> header = record.construct_record_buffer();

  if (sync_mode == SyncMode::SYNC){
    commitlog->flush(move(header), record.get_size());
  } else if (sync_mode == SyncMode::BATCHED) {
    std::unique_ptr<char []> test_buffer = std::make_unique<char []>(2000);
    if (log_buffer_offset + record.get_size() >= LOG_BUFFER_SIZE) {
      // not enough space in buffer, hence we have to flush first
      run_flush_thread(); // it would be put to sleep until a flush is required
      need_flush = true;
      flush_cv.notify_one(); // wake flush thread up
      append_cv.wait(latch, [&] {
          return log_buffer_offset == 0;
      });
    }
    memcpy(log_buffer.get() + log_buffer_offset, header.get(), record.get_size());
  }
  log_buffer_offset += record.get_size();
  last_lsn = record.get_lsn();
  lsn.store(++lsn);
//  std::cout << "log appended successfully with lsn = " << record.get_lsn() << " and size" << record.get_size()<< std::endl;
  // check if we surpassed the max_size
  if(commitlog->get_current_size()>= commitlog->get_max_size()){
    commitlog->update_active_status(false);
    logs_to_flush.push(move(commitlog));
    commitlog.reset(); // not sure if this is redundant after moving the pointer ownership
    commitlog = create_new_log_file();
    //TODO: notify the DB to flush the current file
  }
  return lsn-1;
}

void CommitlogManager::run_flush_thread() {
  if (sync_mode == SyncMode::BATCHED){

flush_buffer = std::make_unique<char []>(LOG_BUFFER_SIZE);
  flush_thread = new std::thread ( [&] {
    std::unique_lock<std::mutex> latch(mutex_latch);
    flush_cv.wait_for(latch, LOG_TIMEOUT, [&] {return need_flush.load();});

    flush_buffer_size = 0; //TODO: check if the flush buffer is not empty and handle that case, rn we over-write its contents
    if (log_buffer_offset > 0){
      std::swap(log_buffer, flush_buffer);
      std::swap(log_buffer_offset, flush_buffer_size);
      commitlog->flush(move(flush_buffer), flush_buffer_size);
      flush_buffer_size = 0;
      set_persistent_lsn(last_lsn);
    }

    need_flush = false;
    append_cv.notify_all();
  }); }

  else if (sync_mode == SyncMode::SYNC){
  // No threads required since we will block the commit until the disk write is done anyway.
  commitlog->flush(move(log_buffer), log_buffer_offset);
  }// TODO:  else { write to logs Invalid sync mode}
  }


void CommitlogManager::force_flush() {
  std::unique_lock <std::mutex> latch(mutex_latch);
  run_flush_thread();
  need_flush = true;
  flush_cv.notify_one(); //let flush thread wake up.
  append_cv.wait(latch, [&] { return !need_flush.load(); }); //block append thread
}

std::unique_ptr<Commitlog> CommitlogManager::create_new_log_file() {
  if (commitlog != nullptr) {
    return move(commitlog);
  } else {
    // Naming the log files
    const char *prefix = "dibibase_log_file_";
    const char *suffix = ".log";
    std::string fullname(prefix + std::to_string(lsn) + suffix);

    std::unique_ptr<Commitlog> log = std::make_unique<Commitlog>(fullname.c_str());
    return move(log);
  }
}

void CommitlogManager::apply_logs_queue(std::queue<std::unique_ptr<Commitlog>> &log_queue) {
  // making sure no records are still in memory
  force_flush();
  // marking the current log file as inactive so that it joins the recovery queue, and start a new log file
  commitlog->update_active_status(false);
  commitlog.reset();
  commitlog = create_new_log_file();

  while (!log_queue.empty()) {
    std::unique_ptr<Commitlog> log = move(log_queue.front());

    int fd = log->get_file_descriptor();
    lseek(fd, Commitlog::COMMITLOG_HEADER_SIZE, SEEK_SET); // The first position after the file header
    std::unique_ptr<char[]> buffer(new char[2000]);

    size_t offset = 1; // dummy value, doesn't matter unless it's 0
    int32_t size;
    while (offset != 0) { // read returns 0 if we reached EOF

      offset = read(fd, buffer.get(), 4);

      if (offset == -1) {
        // read returned an error
        break;
        // TODO: log the error
      }
      // getting the size of the record
      memcpy(reinterpret_cast<unsigned char *>(&size), buffer.get(), sizeof(int32_t));
      //read the entire record
      if (size < 4)
        break; //TODO: log the error as invalid record

      offset = read(fd, buffer.get(), size - 4);

      // create a record from the buffer
      std::unique_ptr<char[]> temp_buffer(new char[2000]);
      std::swap(temp_buffer,buffer);
      LogRecord record = LogRecord(move(temp_buffer));
      db->handle_record(record);
    }
    mark_archived(move(log)); // archived log records are no longer needed
    logs_to_delete.push(move(log));
    log_queue.pop();

  }
}

void CommitlogManager::apply_all_logs() {
  // logs to flush queue contains all unarchived log records
  apply_logs_queue(logs_to_flush);
}

void CommitlogManager::apply_logs_from_lsn(int32_t recovery_lsn) {

  // queue of logs to be recovered
  std::queue<std::unique_ptr<Commitlog>> log_queue;

  // iterating over files in a log directory
  DIR *d;
  struct dirent *dir;
  d = opendir(default_directory.c_str());
  if (d) {
    while ((dir = readdir(d)) != nullptr) {
      if (dir->d_type == DT_REG) { // filtering the regular files from directories or system/hidden files
        std::string file = std::string(dir->d_name);

        if (file.substr(0,18)== "dibibase_log_file_"){ // checking that a file has Dibibase log files naming conventions
          int file_starting_lsn = std::stoi(file.substr(18,file.size()-4)); // get the starting lsn of a log file
          if (file_starting_lsn >= recovery_lsn){ // only files larger than recovery lsn should be added to the queue
          std::unique_ptr<Commitlog> log = std::make_unique<Commitlog>(file.c_str());
          log_queue.push(move(log));
        }
        }
      }
    }
    closedir(d);
  }
  apply_logs_queue(log_queue);

}

