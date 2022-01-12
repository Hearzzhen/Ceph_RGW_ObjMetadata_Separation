#ifndef CEPH_RGW_OBJMETA_H
#define CEPH_RGW_OBJMETA_H

#include "include/buffer.h"
#include "common/OperateKV.h"
#include "cls/rgw/cls_rgw_types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

struct obj_omap_meta {
  CephContext* cct;
  RGWObjCategory category;
  uint64_t size;
  ceph::real_time mtime;
  string etag;
  string owner; 
  string owner_display_name;
  string content_type;
  uint64_t accounted_size;
  string user_data;
  string storage_class;
  bool appendable;

  obj_omap_meta(CephContext *_cct) : 
	  cct(_cct), category(RGWObjCategory::None), size(0), accounted_size(0), appendable(false) { }

  obj_omap_meta() : 
	  category(RGWObjCategory::None), size(0), accounted_size(0), appendable(false) { }

  void encode(bufferlist &bl) const {
	ENCODE_START(1, 1, bl);
	encode(category, bl);
	encode(size, bl);
	encode(mtime, bl);
    encode(etag, bl);
	encode(owner, bl);
	encode(owner_display_name, bl);
	encode(content_type, bl);
	encode(accounted_size, bl);
	encode(user_data, bl);
	encode(storage_class, bl);
	encode(appendable, bl);
	ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl) {
	DECODE_START(1, bl);
	decode(category, bl);
	decode(size, bl);
	decode(mtime, bl);
	decode(etag, bl);
	decode(owner, bl);
	decode(owner_display_name, bl);
	decode(content_type, bl);
	decode(accounted_size, bl);
	decode(user_data, bl);
	decode(storage_class, bl);
	decode(appendable, bl);
	DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(obj_omap_meta)

struct obj_omap {
  CephContext *cct;
  string name;
  string instance;
  int64_t pool;
  uint64_t epoch;
  string locator;
  bool exists;
  obj_omap_meta meta;
  string tag;
  uint16_t flags;

  obj_omap(CephContext *_cct) :
	cct(_cct), exists(false), flags(0) {}

  obj_omap() : exists(false), flags(0) {}

  ~obj_omap() {}

  void encode(bufferlist &bl) const {
	ENCODE_START(1, 1, bl);
	encode(name, bl);
	encode(epoch, bl);
	encode(instance, bl);
	encode(exists, bl);
	encode(meta, bl);
	encode(locator, bl);
	encode(pool, bl);
	encode(tag, bl);
	encode(flags, bl);
	ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl) {
	DECODE_START(1, bl);
	decode(name, bl);
	decode(epoch, bl);
	decode(instance, bl);
	decode(exists, bl);
	decode(meta, bl);
	decode(locator, bl);
	decode(pool, bl);
	decode(tag, bl);
	decode(flags, bl);
	DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(obj_omap)

struct RGW_ObjMeta {
  string obj_name;
  map<std::string, bufferlist> combine_meta;

  explicit RGW_ObjMeta() {}
  explicit RGW_ObjMeta(const string& _obj_name) : obj_name(_obj_name) {}
  ~RGW_ObjMeta() {}

  void set_obj_name(const string& obj_name) {
	this->obj_name = obj_name;
  }
  void insert_to_metamap(const std::string& metaname, const bufferlist& metavalue) {
	combine_meta.insert(pair<std::string, bufferlist>(metaname, metavalue));
  }
};
#undef dout_subsys

#endif
