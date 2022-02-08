##############################################################
#               CMake Project Wrapper Makefile               #
############################################################## 
CC = g++
CFLAGS = -std=c++14 -g -Wall
TAR_NAME = team_name_sharma_syakhroza_vujnovich_BufferPool.tar.gz

all:
	cd src;\
	$(CC) $(CFLAGS) *.cpp exceptions/*.cpp -I. -o badgerdb_main
clean:
	cd src;\
	rm -f badgerdb_main test.?

format:
	find . \( -iname '*.h' -o -iname '*.cpp' \) -exec clang-format -style=Google -i {} \;

docs:
	doxygen Doxyfile
tar:
	tar -czvf $(TAR_NAME) --exclude="*.tar.gz" --exclude="*.pdf" --exclude="docs" --exclude="README*" *
