SHELL=/usr/bin/env bash

.PHONY: all clean destroy lab1-debug debug release format shell cloc

all: debug

# 清除编译产生的文件
clean:
	rm -rf build

# 清除数据库文件
destroy:
	rm -rf huadb_data huadb_test

lab1-debug:
	mkdir -p ./build/debug && \
	cd build/debug && \
	cmake ../.. -DCMAKE_BUILD_TYPE=Debug -DSIMPLE_CATALOG=1 && \
	cmake --build .

debug:
	mkdir -p ./build/debug && \
	cd build/debug && \
	cmake ../.. -DCMAKE_BUILD_TYPE=Debug && \
	cmake --build .

release:
	mkdir -p ./build/debug && \
	cd build/debug && \
	cmake ../.. -DCMAKE_BUILD_TYPE=Release && \
	cmake --build .

format:
	@for file in $(shell find src test -name '*.h' -o -name '*.cpp'); do \
		clang-format -style=file:.clang-format $$file -i; \
	done

lab%-only:
	./build/debug/bin/sqllogictest test/lab$*/*.test

lab%:
	./build/debug/bin/sqllogictest test/lab{0..$*}/*.test

shell:
	./build/debug/bin/shell

server:
	./build/debug/bin/server

client:
	./build/debug/bin/client

cloc:
	@cloc $(shell find src test -name '*.h' -or -name '*.cpp' -not -name node_tag_to_string.cpp)

%:
	./build/debug/bin/sqllogictest test/$@*
