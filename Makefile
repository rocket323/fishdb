prog = fishdb 
src := $(shell find . -name "*.cpp" -print)

$prog:
	g++ -g --std=c++0x ${src} -Wall -o ${prog}

.PHONY: clean

clean:
	rm -f *.o ${prog}

