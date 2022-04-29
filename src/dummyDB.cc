
//#include "CommitlogManager.hh"
#include "CommitlogManager.cc"
#include "src/log_record.cc"
#include "../include/dummyDB.hh"
#pragma once

dummyDB::dummyDB(CommitlogManager *cm) {
  clm = cm;
  clm->register_db(this);
}
dummyDB::~dummyDB() {
  delete clm;
}
std::string dummyDB::get(int key){
    return kv[key];
}

void dummyDB::insert(int key, std::string value) {
  // TODO: Write to the logs here.
  LogRecord record = LogRecord(0, LogRecordType::INSERT, key,value.size(), value);
  clm->append_log_record(record);
  kv[key] = value;
}


bool dummyDB::is_empty(){
  return kv.empty();
}

void dummyDB::update(int key, std::string value){
  LogRecord record = LogRecord(0, LogRecordType::UPDATE, 1, key, value.size(), value);
  clm->append_log_record(record);
  kv[key] = value;
}

void dummyDB::delete_record(int key) {
  LogRecord record = LogRecord(0, LogRecordType::DELETE, 1, key);
  clm->append_log_record(record);

  kv.erase(key);
}

std::string dummyDB::to_string() const {
  std::stringstream os;
  for (auto const& x : kv){
    os << x.first << ":" << x.second << std::endl;

  }
  return os.str();
}


void dummyDB::handle_record(const LogRecord &record) {

  if (record.get_type() == LogRecordType::INSERT){
    kv[record.get_table()] = record.get_data();
  } else if (record.get_type() == LogRecordType::UPDATE){
    kv[record.get_tuple()] = record.get_data();

  } else if(record.get_type() == LogRecordType::DELETE){
    kv.erase(record.get_tuple());
  }

}
