lib= libfishdb.a
src := $(shell find . \( -path "./examples" -o -path "./tests" \) -prune -o -name "*.cpp" -print)
test_src := $(shell find tests -name "*.cpp" -print)
obj := $(patsubst %.cpp, %.o, $(src))

all: ${lib} examples test

${lib}:
	g++ -g -c --std=c++0x -I./ ${src} -Wall 
	ar crv libfishdb.a ${obj}

examples: ${lib}
	g++ -g --std=c++0x -I./ examples/ex_basic.cpp -o examples/ex_basic ${lib} -Wall
	g++ -g --std=c++0x -I./ examples/ex_iter.cpp -o examples/ex_iter ${lib} -Wall

test: ${lib}
	g++ -g --std=c++0x -I./ -o fdb_test ${test_src} ${lib} -Wall

.PHONY: clean

clean:
	rm -f *.o ${lib} examples/ex_basic examples/ex_iter fdb_test

