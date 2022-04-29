#include <iostream>
#include <unordered_map>
#include <string>
#include <sstream>
//#include "CommitlogManager.hh"
#ifndef COMMITLOG_PROTOTYPE_DUMMYDB_HH
#define COMMITLOG_PROTOTYPE_DUMMYDB_HH
#pragma once

class CommitlogManager;
class dummyDB {
public:
    explicit dummyDB(CommitlogManager *cm);
    ~dummyDB();
    std::string get(int key);
    void insert(int key, std::string value);
    void delete_record (int key);
    void update (int key, std::string value);
    // a workaround to avoid logging while constructing records from logs TODO:refactor this
    void insert_without_logging(int key, std::string value);
    void delete_record_without_logging (int key);
    void update_without_logging (int key, std::string value);

    // insert/update/delete a record based on its type and data
    void handle_record(const LogRecord &record);
    bool is_empty();

    //for testing
    std::string to_string() const;
    CommitlogManager *clm;

private:
    std::unordered_map<int, std::string> kv;
};


#endif //COMMITLOG_PROTOTYPE_DUMMYDB_HH
