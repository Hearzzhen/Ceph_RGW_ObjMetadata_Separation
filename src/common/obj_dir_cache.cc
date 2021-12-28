#include <error.h>
#include "obj_dir_cache.h"

#define dout_subsys ceph_subsys_rgw

void ObjDirCache::put(const std::string& name/*, ObjDirInfo& info*/) {
  lock.get_write();

  if (!enabled) {
    lock.put_write();
	return;
  }

  ldout(cct, 10) << "rgw_objdir_cache put: name = " << name << dendl;
  lock.put_write();
  // not in hot lru, should be inserted to warm lru, and warm count++, when count == n, remove it and insert to hot lru.
  int get_res = get(name);
  lock.get_write();
  if (-ENOENT == get_res) {
	ldout(cct, 10) << "rgw_objdir_cache not found " << name << " in hot lru." << dendl;
	auto[warm_iter, inserted] = warm_cache_map.emplace(name, ObjDirWarmEntry{});
	ObjDirWarmEntry& warm_entry = warm_iter->second;
    warm_entry.info.time_added = ceph::coarse_mono_clock::now();
	if (inserted) { //count must be 0
	  warm_entry.lru_iter = warm_lru.end();
	  warm_entry.count++;
	  touch_warm_lru(name, warm_entry, warm_entry.lru_iter);
	  ldout(cct, 10) << "rgw_objdir_cache first put to warm lru done. name = " << name << " warm_entry.count = " << warm_entry.count << dendl;
	} else { //count > 0
	  if (warm_entry.count + 1 == (size_t)(cct->_conf->rgw_obj_dir_cache_warm_entry_max_count)) {//default: 5
	    auto remove_iter = warm_cache_map.find(name);
		if (remove_iter == warm_cache_map.end()) {
		  ldout(cct, 10) << "rgw_objdir_cache name = " << name << " is not in warm lru." << dendl;
		  return;
		}
		ldout(cct, 10) << "rgw_objdir_cache warm_count reached " << cct->_conf->rgw_obj_dir_cache_warm_entry_max_count << ", removing " << name << " from warm lru." << dendl;
		remove_warm_lru(name, remove_iter->second.lru_iter);
		warm_cache_map.erase(remove_iter);

		auto[hot_iter, hot_inserted] = hot_cache_map.emplace(name, ObjDirHotEntry{});
		ObjDirHotEntry& hot_entry = hot_iter->second;
		hot_entry.info.time_added = ceph::coarse_mono_clock::now();
		if (hot_inserted) {
		  hot_entry.lru_iter = hot_lru.end();
		}
		touch_hot_lru(name, hot_entry, hot_entry.lru_iter);
		
		ldout(cct, 10) << "rgw_objdir_cache warm_count reached 5, put to hot lru done. name = " << name << dendl;
	  } else if (warm_entry.count + 1 < (size_t)(cct->_conf->rgw_obj_dir_cache_warm_entry_max_count)) {
		warm_entry.count++;
		touch_warm_lru(name, warm_entry, warm_iter->second.lru_iter);

		ldout(cct, 10) << "rgw_objdir_cache put to warm lru done. name = " << name << " warm_entry.count = " << warm_entry.count << dendl;
	  }
	}
  } else if(0 == get_res) {
    // it is in hot lru, handle it with normal lru.
	ldout(cct, 10) << "rgw_objdir_cache name =  " << name << " is already in hot lru." << dendl;
	auto[hot_iter, hot_inserted] = hot_cache_map.emplace(name, ObjDirHotEntry{});
	ObjDirHotEntry& hot_entry = hot_iter->second;
	hot_entry.info.time_added = ceph::coarse_mono_clock::now();
	if (hot_inserted) {
	  hot_entry.lru_iter = hot_lru.end();
	}

	touch_hot_lru(name, hot_entry, hot_entry.lru_iter);
	ldout(cct, 10) << "rgw_objdir_cache put to hot lru done. name = " << name << dendl;
  }
  lock.put_write();
}

int ObjDirCache::get(const std::string& name/*, ObjDirInfo& info*/) {
  RWLock::RLocker l(lock);

  if(!enabled) {
	return -ENOENT;
  }

  auto hot_iter = hot_cache_map.find(name);
  if (hot_iter == hot_cache_map.end()) {
	ldout(cct, 10) << "rgw_objdir_cache hot_lru get: name = " << name << " : miss" << dendl;
	return -ENOENT;
  }

  if (hot_expiry.count() && (ceph::coarse_mono_clock::now() - hot_iter->second.info.time_added) > hot_expiry) {
	ldout(cct, 10) << "rgw_objdir_cache hot_lru get: name = " << name << " : expiry miss" << dendl;

	lock.unlock();
    lock.get_write();

	hot_iter = hot_cache_map.find(name);
	if (hot_iter != hot_cache_map.end()) {
	  remove_hot_lru(name, hot_iter->second.lru_iter);
	  hot_cache_map.erase(hot_iter);
	  ldout(cct, 10) << "rgw_objdir_cache hot_lru removed expiry name = " << name << dendl;
	}

	ldout(cct, 10) << "rgw_objdir_cache hot_lru re-add: name = " << name << dendl;
	auto[new_iter, hot_inserted] = hot_cache_map.emplace(name, ObjDirHotEntry{});
	ObjDirHotEntry& hot_entry = new_iter->second;
	hot_entry.info.time_added = ceph::coarse_mono_clock::now();
	if(hot_inserted) {
	  hot_entry.lru_iter = hot_lru.end();
	}

	touch_hot_lru(name, hot_entry, hot_entry.lru_iter);
	/*ObjDirInfo& src = new_iter->second.info;  //now, our info has no member.*/
	/*info = src;*/
	ldout(cct, 10) << "rgw_objdir_cache hot_lru re-add name = " << name << " done" << dendl;
	return 0;
  }

  ObjDirHotEntry *entry = &hot_iter->second;
  if (hot_lru_counter - entry->lru_promotion_ts > hot_lru_window) {
	ldout(cct, 10) << "rgw_objdir_cache hot_lru get : touching lru, hot_lru_counter = " << hot_lru_counter << " promotion_ts = " << entry->lru_promotion_ts << dendl;
	lock.unlock();
	lock.get_write();

	hot_iter = hot_cache_map.find(name);
	if (hot_iter == hot_cache_map.end()) {
	  ldout(cct, 10) << "lost race! rgw_objdir_cache hot_lru get: name = " << name << " : miss" << dendl;
	  return -ENOENT;
	}

	entry = &hot_iter->second;
	if (hot_lru_counter - entry->lru_promotion_ts > hot_lru_window) {
	  touch_hot_lru(name, *entry, hot_iter->second.lru_iter);
	}
  }

  /*ObjDirInfo& src = hot_iter->second.info;  //now, our info has no member.*/
  /*info = src;*/
  ldout(cct, 10) << "rgw_objdir_cache hot_lru get: name = " << name << " : hit" << dendl;

  return 0;
}

//only remove hot_lru, because the same dir structure may be used by other object.
bool ObjDirCache::remove(const std::string& name) {
  RWLock::WLocker l(lock);
  
  if (!enabled)
	return false;

  auto hot_iter = hot_cache_map.find(name);
  if (hot_iter == hot_cache_map.end()) {
	return false;
  }

  ldout(cct, 10) << "rgw_objdir_cache hot_lru removing " << name << " from cache" << dendl;

  remove_hot_lru(name, hot_iter->second.lru_iter);
  hot_cache_map.erase(hot_iter);

  return true;
}

void ObjDirCache::touch_warm_lru(const string& name, ObjDirWarmEntry& entry, std::list<string>::iterator& warm_lru_iter) {
  while (warm_lru_size > (size_t)cct->_conf->rgw_obj_dir_cache_warm_lru_size) {
	auto warm_iter = warm_lru.begin();
	if ((*warm_lru_iter).compare(name) == 0) {
	  break;
	}

	auto warm_map_iter = warm_cache_map.find(*warm_iter);
    ldout(cct, 10) << "removing obj dir cache name = " << name << " from warm lru." << dendl;
	if (warm_map_iter != warm_cache_map.end()) {
	  warm_cache_map.erase(warm_map_iter);   //TODO:Maybe we can find minimum count to erase it!
	}
	warm_lru.pop_front();
	warm_lru_size--;
  }

  if (warm_lru_iter == warm_lru.end()) {
	warm_lru.push_back(name);
	warm_lru_size++;
	warm_lru_iter--;
	ldout(cct, 10) << "adding " << name << " to obj dir cache warm LRU end." << dendl;
  } else {
	ldout(cct, 10) << "moving " << name << " to obj dir cache warm LRU end." << dendl;
	warm_lru.erase(warm_lru_iter);
	warm_lru.push_back(name);
	warm_lru_iter = warm_lru.end();
	--warm_lru_iter;
  }

  warm_lru_counter++;
  entry.lru_promotion_ts = warm_lru_counter;
}

void ObjDirCache::touch_hot_lru(const string& name, ObjDirHotEntry& entry, std::list<string>::iterator& hot_lru_iter) {
  while (hot_lru_size > (size_t)cct->_conf->rgw_obj_dir_cache_hot_lru_size) {
	auto hot_iter = hot_lru.begin();
	if ((*hot_iter).compare(name) == 0) {
	  break;
	}

	auto hot_map_iter = hot_cache_map.find(*(hot_iter));
	ldout(cct, 10) << "removing obj dir cache name = " << name << " from hot lru." << dendl;
	if (hot_map_iter != hot_cache_map.end()) {
	  hot_cache_map.erase(hot_map_iter);
	}
	hot_lru.pop_front();
	hot_lru_size--;
  }

  if (hot_lru_iter == hot_lru.end()) {
	hot_lru.push_back(name);
	hot_lru_size++;
	hot_lru_iter--;
	ldout(cct, 10) << "adding " << name << " to obj dir cache hot LRU end." << dendl;
  } else {
	ldout(cct, 10) << "moving " << name << " to obj dir cache hot LRU end." << dendl;
	hot_lru.erase(hot_lru_iter);
	hot_lru.push_back(name);
	hot_lru_iter = hot_lru.end();
	--hot_lru_iter;

	hot_lru_counter++;
	entry.lru_promotion_ts = hot_lru_counter++;
  }
}

void ObjDirCache::remove_warm_lru(const string& name, std::list<std::string>::iterator& lru_iter) {
  if (lru_iter == warm_lru.end()) {
	return;
  }

  warm_lru.erase(lru_iter);
  warm_lru_size--;
  lru_iter = warm_lru.end();
}

void ObjDirCache::remove_hot_lru(const string& name, std::list<std::string>::iterator& lru_iter) {
  if (lru_iter == hot_lru.end()) {
	return;
  }

  hot_lru.erase(lru_iter);
  hot_lru_size--;
  lru_iter = hot_lru.end();
}

void ObjDirCache::set_enabled(bool status) {
  RWLock::WLocker l(lock);

  enabled = status;

  if (!enabled) {
	do_invalidate_all();
  }
}

void ObjDirCache::do_invalidate_all() {
  hot_cache_map.clear();
  warm_cache_map.clear();
  hot_lru.clear();
  warm_lru.clear();

  hot_lru_size = 0;
  hot_lru_counter = 0;
  hot_lru_window = 0;

  warm_lru_size = 0;
  warm_lru_counter = 0;
  warm_lru_window = 0;
}
