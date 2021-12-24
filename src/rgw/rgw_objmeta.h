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

class RGW_ObjMeta {
private:
  map<std::string, bufferlist> objmeta_kv; //string is dir/obj structure, bufferlist is metadata.
  map<std::string, bufferlist> combine_meta; //string is xattr like 'user.rgw.xxxx' and omapval named 'omapvals', bufferlist is xattr's value and omap value
  //OperateKV *operateKV;

  class CephContext *cct;
public:
  explicit RGW_ObjMeta(CephContext* _cct);
  explicit RGW_ObjMeta() {}
  explicit RGW_ObjMeta(const RGW_ObjMeta& _obj_meta) {
	this->objmeta_kv = _obj_meta.objmeta_kv;
	this->combine_meta = _obj_meta.combine_meta;
	//this->operateKV = _obj_meta.operateKV;
  }
  ~RGW_ObjMeta();

  RGW_ObjMeta& operator=(const RGW_ObjMeta& _obj_meta) {
	this->objmeta_kv = _obj_meta.objmeta_kv;
	this->combine_meta = _obj_meta.combine_meta;
	//this->operateKV = _obj_meta.operateKV;
	return *this;
  }

  void init();
  void shutdown();
  void insert_to_metamap(const std::string& metaname, const bufferlist& metavalue);
  void insert_to_kv(const std::string& objkey, const bufferlist& combine_metavalue);

  map<std::string, bufferlist>& get_combine_meta() {
	return combine_meta;
  }

  map<std::string, bufferlist>& get_objmeta_kv() {
	return objmeta_kv;
  }

  void clear_combine_meta() {
	if (!combine_meta.empty()) {
	  combine_meta.clear();
	}
  }

  void clear_objmeta_kv() {
	if (!objmeta_kv.empty()) {
	  objmeta_kv.clear();
	}
  }

  void encode(bufferlist &bl) const {
	ENCODE_START(1, 1, bl);
	encode(combine_meta, bl);
	encode(objmeta_kv, bl);
	ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl) {
	DECODE_START(1, bl);
	decode(combine_meta, bl);
	decode(objmeta_kv, bl);
	DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(RGW_ObjMeta)

#undef dout_subsys

#endif
