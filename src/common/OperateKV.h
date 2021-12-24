#ifndef CEPH_OPERATEKV_H
#define CEPH_OPERATEKV_H

#include "include/Context.h"
#include "common/Thread.h"
#include "common/ceph_mutex.h"
#include "common/Cond.h"

class CephContext;

class OperateKV {
private:
  CephContext *cct;
  ceph::mutex operateKV_lock;
  ceph::condition_variable operateKV_cond;
  ceph::condition_variable operateKV_empty_cond;
  vector<map<std::string, bufferlist>> operateKV_queue;

  string thread_name;

  bool operateKV_stop;
  bool operateKV_running;
  bool operateKV_empty_wait;

  void *operateKV_thread_entry();

  struct OperateKVThread : public Thread {
    OperateKV *op_kv;
    explicit OperateKVThread(OperateKV *o) : op_kv(o) {}
    void* entry() override { return op_kv->operateKV_thread_entry(); }
  } operateKV_thread;

public:
  void queue(map<std::string, bufferlist>& m) {
    std::unique_lock ul(operateKV_lock);
	if (operateKV_queue.empty()) {
	  operateKV_cond.notify_all();
	}
	operateKV_queue.push_back(m);
  }

  void start();

  void stop();

  void wait_for_empty();

  explicit OperateKV(CephContext *cct_) :
    cct(cct_), operateKV_lock("OperateKV::operateKV_lock"), thread_name("operateKV"),
	operateKV_stop(false), operateKV_running(false), operateKV_empty_wait(false), operateKV_thread(this) {}

  ~OperateKV() {}

  void handle_str(std::string& input, vector<string>& result, bool has_flag);
  void recursive(std::string& input, int nums, int count, vector<string>& result, bool has_flag);
  vector<string> extract(std::string& input);
  bool has_inserted(std::string head_str);
};

#endif
