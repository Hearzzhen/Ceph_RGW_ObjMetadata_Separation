#ifndef CEPH_OBJ_DIR_CACHE_H
#define CEPH_OBJ_DIR_CACHE_H

#include <string>
#include <map>
#include <unordered_map>
#include "include/utime.h"
#include "include/types.h"
#include "common/RWLock.h"
#include "include/Context.h"

class CephContext;

struct ObjDirInfo {
  ceph::coarse_mono_time time_added;
  //map<string> dirs; //dirs need implement synchronization, phase II.
};

struct ObjDirWarmEntry {
  ObjDirInfo info;
  std::list<string>::iterator lru_iter;
  uint64_t lru_promotion_ts;
  uint64_t count;

  ObjDirWarmEntry() : lru_promotion_ts(0), count(0) { }
};

struct ObjDirHotEntry {
  ObjDirInfo info;
  std::list<string>::iterator lru_iter;
  uint64_t lru_promotion_ts;
};

// object dir structure cache, use lru-2 manage.
class ObjDirCache {
private:
  std::unordered_map<std::string, ObjDirWarmEntry> warm_cache_map;
  std::list<string> warm_lru;
  unsigned long warm_lru_size;
  unsigned long warm_lru_counter;
  unsigned long warm_lru_window;

  std::unordered_map<std::string, ObjDirHotEntry> hot_cache_map;
  std::list<string> hot_lru;
  unsigned long hot_lru_size;
  unsigned long hot_lru_counter;
  unsigned long hot_lru_window;

  RWLock lock;
  CephContext *cct;

  bool enabled; //enable when ObjMetaCache is enabled
  ceph::timespan hot_expiry; //hot_lru_time should set a long value and long length, warm_lru no need to set time, only set length.

public:
  ObjDirCache() : warm_lru_size(0), warm_lru_counter(0), warm_lru_window(0), 
				hot_lru_size(0), hot_lru_counter(0), hot_lru_window(0), lock("ObjDirCache"), cct(NULL), enabled(false) { }
  ~ObjDirCache() { }

  void touch_warm_lru(const string& name, ObjDirWarmEntry& entry, std::list<string>::iterator& warm_lru_iter);
  void touch_hot_lru(const string& name, ObjDirHotEntry& entry, std::list<string>::iterator& hot_lru_iter);
  void remove_warm_lru(const string& name, std::list<std::string>::iterator& lru_iter);
  void remove_hot_lru(const string& name, std::list<std::string>::iterator& lru_iter);
  void put(const std::string& name/*, ObjDirInfo& info*/);
  int get(const std::string& name/*, ObjDirInfo& info*/);
  bool remove(const std::string& name);

  void set_ctx(CephContext *_cct) {
    cct = _cct;
	hot_lru_window = cct->_conf->rgw_obj_dir_cache_hot_lru_size / 2;
	hot_expiry = std::chrono::seconds(cct->_conf.get_val<uint64_t>("rgw_obj_dir_cache_hot_expiry_interval")); 
  }
  void set_enabled(bool status);
  void do_invalidate_all();
};

#endif
