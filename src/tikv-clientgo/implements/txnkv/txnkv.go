// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
	unsigned char* k;
	unsigned char* v;
	int klen;
	int vlen;
} KV_return;

extern unsigned char* mallocUChar(int size);
extern void copyUChar(unsigned char* str, unsigned char* v, int len);
extern KV_return** mallocKVStruct(int limit);
extern void copyKVStruct(KV_return** kv_return, const unsigned char* k, const unsigned char* v, int index, int klen, int vlen);
extern void FreeKVStruct(KV_return** kv_return, int limit);
extern char* getKVStructKey(KV_return** kv, int index);
extern char* getKVStructVal(KV_return** kv, int index);
extern char* getKeys(char** keys, int index);
*/
import "C"
import (
	"context"
	//"flag"
	"log"
	"fmt"
	"os"
	"unsafe"
	//"reflect"

	"github.com/tikv/client-go/v2/tikv"
)

// KV represents a Key-Value pair.
type KV struct {
	K *C.char
	V *C.char
}

func (kv KV) String() string {
	return fmt.Sprintf("%s => %s (%v)", C.GoString(kv.K), C.GoString(kv.V), kv.V)
}

var (
	client *tikv.KVStore
	//pdAddr = flag.String("pd", "10.1.172.118:2379", "pd address")
)

func initOS() {
	logFile, err := os.OpenFile("/var/log/ceph/txnkv.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    if err != nil {
		fmt.Println("open log file failed, err:", err)
		return
	}
	log.SetOutput(logFile)
	log.SetPrefix("[txnkv]")
	log.SetFlags(log.Lshortfile |log.Lmicroseconds | log.Ldate)

	log.Println("initOS")
	pdAddr := os.Getenv("PD_ADDR")
        if pdAddr != "" {
                os.Args = append(os.Args, "-pd", pdAddr)
        }
    //    flag.Parse()
	log.Println("initOS done!")
}

// Init initializes information.
//export initStore
func initStore(ip_address string) {
	initOS()
	var err error
	//client, err = tikv.NewTxnClient([]string{*pdAddr})
	client, err = tikv.NewTxnClient([]string{ip_address})
	if err != nil {
		panic(err)
	}
	log.Println("initStore done!")
}

// key1 val1 key2 val2 ...
//export putsKV
func putsKV(key string, value string) int {
	fmt.Println("putsKV!")
	tx, err := client.Begin()
        if err != nil {
                return -1
        }
	err = tx.Set([]byte(key), []byte(value))
	if err != nil {
                return -1
        }
	err = tx.Commit(context.Background())
        if err != nil {
		return -1
	}
	return 0
}

//export putsKVMap
func putsKVMap(kv **C.KV_return, size int) int {
	fmt.Println("putsKVMap!")
	tx, err := client.Begin()
	if err != nil {
		fmt.Println("Begin Error!")
		return -1
	}

	for i := 0; i < size; i++ {
		k := C.getKVStructKey(kv, C.int(i))
		v := C.getKVStructVal(kv, C.int(i))
		key, val := []byte(C.GoString(k)), []byte(C.GoString(v))
		fmt.Println("key:", C.GoString(k))
		fmt.Println("val:", C.GoString(v))
		err = tx.Set([]byte(key), []byte(val))
		if err != nil {
			fmt.Println("Set Error!")
			return -1
		} 
	}
	
	err = tx.Commit(context.Background())
	if err != nil {
		fmt.Println("Commit Error!")
		return -1
	}
	return 0
}

//export getKV
func getKV(k string) (value *C.uchar, length int, e int) {
	//value = C.mallocUChar(102400);
	tx, err := client.Begin()
	if err != nil {
		e = -1
		fmt.Println("tx failed!")
		return value, 0, e
	}
	v, err := tx.Get(context.TODO(), []byte(k))
	if err != nil {
		e = -1
		fmt.Println("Get %s failed!", k)
		return value, 0, e
	}
	
	value = (*C.uchar)(unsafe.Pointer(C.CBytes(v)))
	//C.copyChar(value, (*C.char)((C.CString(string(v)))))
	//C.copyUChar(value, (*C.uchar)(unsafe.Pointer(C.CBytes)(v)), (C.int)(len(v)))
	log.Println("Get success! v: ", k, string(v), v)
	return value, len(v), 0
}

//export freeCBytes
func freeCBytes(ptr *C.char) {
	C.free(unsafe.Pointer(ptr))
}

//export delKey
func delKey(key string) (e int) {
    tx, err := client.Begin()
    if err != nil {
	e = -1
        return e
    }

	err = tx.Delete([]byte(key))
	if err != nil {
		e = -1
		return e
	}
         
	tx.Commit(context.Background())
	return 0
}

//export delKeys
func delKeys(keys **C.char, size int) (e int) {
	tx, err := client.Begin()
	if err != nil {
		e = -1
		return e
	}
	
	for i := 0; i < size; i++ {
		key := C.getKeys(keys, C.int(i))
		err = tx.Delete([]byte(C.GoString(key)))
		log.Println("Del Key: ", string(C.GoString(key)))
		if err != nil {
            e = -1
            return e
        }
	}
 
	tx.Commit(context.Background())
	return 0
}

//export scanKV
func scanKV(keyPrefix string, limit int) (ret **C.KV_return, count int, e int) {
	log.Println("Go scanKV IN!")
	tx, err := client.Begin()
	if err != nil {
		e = -1
		return nil, 0, e
	}
	it, err := tx.Iter([]byte(keyPrefix), nil)
	if err != nil {
		e = -1
		return nil, 0, e
	}
	defer it.Close()
	
	//p := C.malloc(C.size_t(10000))
	//defer C.free(p) 

	// make a slice for convenient indexing
	//ret = ([]KV)(ret)[:limit:limit]
	//ret = make([]KV, 10)
	ret = C.mallocKVStruct(C.int(limit))
	i := 0
	for it.Valid() && limit > 0 {
		tk := (*C.uchar)(unsafe.Pointer(C.CBytes(it.Key())))
		tv := (*C.uchar)(unsafe.Pointer(C.CBytes(it.Value())))
		defer C.free(unsafe.Pointer(tk))
		defer C.free(unsafe.Pointer(tv))
		C.copyKVStruct(ret, tk, tv, C.int(i), C.int(len(it.Key())), C.int(len(it.Value())))
		limit--
		i++
		it.Next()
	}
	return ret, i, 0
}

//export FreeKV
func FreeKV(ret **C.KV_return, limit int) {
	C.FreeKVStruct(ret, C.int(limit))
}

func main() {
	/*
	//pdAddr := os.Getenv("PD_ADDR")
	//if pdAddr != "" {
	//	os.Args = append(os.Args, "-pd", pdAddr)
	//}
	//flag.Parse()
	initStore("10.1.172.118:2379")

	// set
	//err := puts([]byte("key1"), []byte("value1"), []byte("key2"), []byte("value2"))
	errno := putsKV("key1", "value1")
	if errno != 0 {
		fmt.Println("input err!")
	}
	errno = putsKV("key2", "value2")
	if errno != 0 {
		fmt.Println("input err!")
	}

	var m map[string]string
	m = make(map[string]string)
	m["key3"] = "value3"
	m["key4"] = "value4"
	//errno = putsKVMap(m)
	//if errno != 0 {
    //            fmt.Println("input err!")
    //    }
	fmt.Println("Input 3/4 success!")

	// get
	//val, errno := getKV("key1")
	//if errno != 0 {
//		fmt.Println("Get err!")
//	}
//	fmt.Println("key1 ", C.GoString(val))
//	fmt.Println("Get success!")
	// scan
	var ret **C.KV_return
	//ret := make([]KV, 10)
	ret, errno = scanKV("key", 10)
	if errno != 0 {
		fmt.Println("Scan err!")
	}
	//for k, v := range ret {
	//	fmt.Println(k)
	//	fmt.Println(v)
	//}

	FreeKV(ret, 10)
	fmt.Println("over!")
	// delete
	//err := dels([]byte("key1"), []byte("key2"))
	//if err != nil {
	//	panic(err)
	//}
	*/
}
