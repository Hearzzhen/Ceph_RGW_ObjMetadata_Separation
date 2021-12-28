#include <error.h>
#include "rgw_objmeta_cache.h"

#define dout_subsys ceph_subsys_rgw

int ObjMetaCache::get(const std::string& name, ObjMetaCacheInfo& info) {
  RWLock::RLocker l(lock);

  if (!enabled)
	return -ENOENT;

  auto iter = objmetacache_map.find(name);
  if (iter == objmetacache_map.end()) {
	ldout(cct, 10) << "obj meta cache get: name = " << name << " : miss" << dendl;
	return -ENOENT;
  }

  if (expiry.count() && (ceph::coarse_mono_clock::now() - iter->second.info.time_added) > expiry) {
    ldout(cct, 10) << "obj meta cache get: name = " << name << " : expiry miss" << dendl;
	lock.unlock();
	lock.get_write();
	iter = objmetacache_map.find(name);
	if (iter != objmetacache_map.end()) {
	  remove_lru(name, iter->second.lru_iter);
	  objmetacache_map.erase(iter);
	}
	return -ENOENT;
  }

  ObjMetaEntry *entry = &iter->second;

  if (lru_counter - entry->lru_promotion_ts > lru_window) {
	ldout(cct, 20) << "obj meta cache get: touching lru, lru_counter = " << lru_counter << " promotion_ts = " << entry->lru_promotion_ts << dendl;
	lock.unlock();
	lock.get_write();

	iter = objmetacache_map.find(name);
	if (iter == objmetacache_map.end()) {
	  ldout(cct, 10) << "lost race! obj meta cache get: name = " << name << " : miss" << dendl;
	  return -ENOENT;
	}

	entry = &iter->second;
	if (lru_counter - entry->lru_promotion_ts > lru_window) {
	  touch_lru(name, *entry, iter->second.lru_iter);
	}
  }

  ObjMetaCacheInfo& src = iter->second.info;
  ldout(cct, 10) << "obj meta cache get: name = " << name << " : hit" << dendl;
  info = src;

  return 0;
}

void ObjMetaCache::put(const std::string& name, ObjMetaCacheInfo& info) {
  RWLock::WLocker l(lock);

  if (!enabled) 
	return;

  ldout(cct, 10) << "rgw_objmeta_cache put: name = " << name << dendl;
  
  auto[iter, inserted] = objmetacache_map.emplace(name, ObjMetaEntry{});
  ObjMetaEntry& entry = iter->second;
  entry.info.time_added = ceph::coarse_mono_clock::now();
  if (inserted) {
    entry.lru_iter = lru.end();
  }

  ObjMetaCacheInfo& target = entry.info;

  touch_lru(name, entry, entry.lru_iter);

  target.objmeta_kv = info.objmeta_kv;
  target.mtime = info.mtime;
  target.size = info.size;
  ldout(cct, 10) << "rgw_objmeta_cache put done: name = " << name << " size = " << info.size << dendl;
}

bool ObjMetaCache::remove(const std::string name) {
  RWLock::WLocker l(lock);

  if (!enabled) {
    return false;
  }

  auto iter = objmetacache_map.find(name);
  if (iter == objmetacache_map.end()) {
	return false;
  }

  ldout(cct, 10) << "removing " << name << " from cache" << dendl;
  //ObjMetaEntry& entry = iter->second;

  remove_lru(name, iter->second.lru_iter);
  objmetacache_map.erase(iter);

  return true;
}

void ObjMetaCache::touch_lru(const string& name, ObjMetaEntry& entry, std::list<std::string>::iterator& lru_iter) {
  while (lru_size > (size_t)cct->_conf->rgw_obj_meta_cache_lru_size) {
    auto iter = lru.begin();
	if ((*iter).compare(name) == 0) {
	  break;
	}

	auto map_iter = objmetacache_map.find(*iter);
	ldout(cct, 10) << "removing obj meta cache name = " << *iter << " from obj meta cache lru." << dendl;
	if (map_iter != objmetacache_map.end()) {
	  //ObjMetaEntry& entry = map_iter->second;
	  objmetacache_map.erase(map_iter);
	}
	lru.pop_front();
	lru_size--;
  }

  if (lru_iter == lru.end()) {
	lru.push_back(name);
	lru_size++;
	lru_iter--;
	ldout(cct, 10) << "adding " << name << " to obj meta cache LRU end." << dendl;
  } else {
	ldout(cct, 10) << "moving " << name << " to obj meta cache LRU end." << dendl;
	lru.erase(lru_iter);
	lru.push_back(name);
	lru_iter = lru.end();
	--lru_iter;
  }

  lru_counter++;
  entry.lru_promotion_ts = lru_counter;
}

void ObjMetaCache::remove_lru(const string& name, std::list<std::string>::iterator& lru_iter) {
  if (lru_iter == lru.end()) {
	return;
  }

  lru.erase(lru_iter);
  lru_size--;
  lru_iter = lru.end();
}

void ObjMetaCache::set_enabled(bool status) {
  RWLock::WLocker l(lock);

  enabled = status;

  if (!enabled) {
	do_invalidate_all();
  }
}

void ObjMetaCache::invalidate_all() {
  RWLock::WLocker l(lock);

  do_invalidate_all();
}

void ObjMetaCache::do_invalidate_all() {
  objmetacache_map.clear();
  lru.clear();

  lru_size = 0;
  lru_counter = 0;
  lru_window = 0;
}
