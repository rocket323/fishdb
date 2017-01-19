lib= libfishdb.a
src := $(shell find . -path "./examples" -prune -o -name "*.cpp" -print)
obj := $(patsubst %.cpp, %.o, $(src))

all: ${lib} examples

${lib}:
	g++ -g -c --std=c++0x -I./ ${src} -Wall 
	ar crv libfishdb.a ${obj}

examples: ${lib}
	g++ -g --std=c++0x -I./ ${src} examples/ex_basic.cpp -o ex_basic ${lib}
	g++ -g --std=c++0x -I./ ${src} examples/ex_iter.cpp -o ex_iter ${lib}

.PHONY: clean

clean:
	rm -f *.o ${lib} ex_basic ex_iter

