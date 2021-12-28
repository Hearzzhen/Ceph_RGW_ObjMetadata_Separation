if (WITH_RADOSGW_TIKV_CLIENTGO)
  execute_process(
  COMMAND
  "sh" "-c"
  "cd ${PROJECT_SOURCE_DIR}/src/tikv-clientgo/implements/txnkv/ && go build -o libtxnkv.so -buildmode=c-shared . && chmod a+x libtxnkv.so && cp -a libtxnkv.so ${CMAKE_BINARY_DIR}/lib/ && cp -a libtxnkv.h ${CMAKE_BINARY_DIR}/include/"
  OUTPUT_VARIABLE TIKV-CLIENTGO_LIBRARY
  ERROR_VARIABLE GO_BUILD_ERRORS
  OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  if (GO_BUILD_ERRORS)
	message(FATAL_ERROR "go build error! " ${GO_BUILD_ERRORS})
  endif()
#set(FCGI_LIBRARY libtxnkv.so)
endif (WITH_RADOSGW_TIKV_CLIENTGO)
