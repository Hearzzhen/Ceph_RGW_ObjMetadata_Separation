#ifndef CEPH_RGWOBJMETA_CACHE_H
#define CEPH_RGWOBJMETA_CACHE_H

#include <string>
#include <map>
#include <unordered_map>
#include "include/utime.h"
#include "include/types.h"
#include "common/RWLock.h"
#include "rgw_rados.h"

class CephContext;

struct ObjMetaCacheInfo {
  map<std::string, bufferlist> objmeta_kv;
  uint64_t size;
  real_time mtime;
  ceph::coarse_mono_time time_added;

  ObjMetaCacheInfo() : size(0) { }
};

struct ObjMetaEntry {
  ObjMetaCacheInfo info;
  std::list<std::string>::iterator lru_iter;
  uint64_t lru_promotion_ts;
  uint64_t gen;

  ObjMetaEntry() : lru_promotion_ts(0), gen(0) { }
};

class ObjMetaCache {
private:
  std::unordered_map<std::string, ObjMetaEntry> objmetacache_map;
  std::list<string> lru;
  unsigned long lru_size;
  unsigned long lru_counter;
  unsigned long lru_window;
  RWLock lock;
  CephContext *cct;

  bool enabled;
  ceph::timespan expiry;

public:
  void touch_lru(const string& name, ObjMetaEntry& entry, std::list<std::string>::iterator& lru_iter);
  void remove_lru(const string& name, std::list<std::string>::iterator& lru_iter);

  ObjMetaCache() : lru_size(0), lru_counter(0), lru_window(0), lock("ObjMetaCache"), cct(NULL), enabled(false) { }
  ~ObjMetaCache() { }

  void put(const std::string& name, ObjMetaCacheInfo& info);
  int get(const std::string& name, ObjMetaCacheInfo& info);
  std::optional<ObjMetaCacheInfo> get(const std::string& name) {
	std::optional<ObjMetaCacheInfo> info{std::in_place};
	auto r = get(name, *info);
	return r < 0 ? std::nullopt : info;
  }

  template<typename F>
  void for_each(const F& f) {
	RWLock::RLocker l(lock);
	if (enabled) {
	  auto now = ceph::coarse_mono_clock::now();
	  for (const auto& [name, entry] : objmetacache_map) {
		if (expiry.count() && (now - entry.info.time_added) < expiry) {
		  f(name, entry);
		}
	  }
	}
  }

  bool remove(const std::string name);

  void set_ctx(CephContext *_cct) {
	cct = _cct;
	lru_window = cct->_conf->rgw_obj_meta_cache_lru_size / 2;
	expiry = std::chrono::seconds(cct->_conf.get_val<uint64_t>("rgw_obj_meta_cache_expiry_interval"));
  }

  void set_enabled(bool status);
  void invalidate_all();
  void do_invalidate_all();
};

#endif
