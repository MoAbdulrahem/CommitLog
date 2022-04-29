#include <iostream>
#include <string>
#include "src/dummyDB.cc"
#include <iterator>
int main(){
  CommitlogManager *clm {new CommitlogManager(SyncMode::BATCHED)};
  dummyDB db = dummyDB(clm);

//  db.insert(1, "First Record");
//  db.update(1, "Updated First Record");
//  db.insert(2, "Second Record");
//  db.delete_record(2);
//  db.insert(3, "Third Record");
//  db.update(3, "Updated Third Record");
//  db.insert(4, "Fourth Record");
//  db.insert(5, "Fifth Record");
//  db.insert(6, "Sixth Record");

//  db.apply_logs("../logs/dibibase_log_file_3.log");

//  db.apply_all_logs();
//  db.clm->apply_all_logs();
  db.clm->apply_logs_from_lsn(20);
  std::cout<< db.to_string();

}