CC=gcc
CFLAGS=-Wall -Wextra -Werror -std=gnu99 -g
SRC=./usertest

fs_test.o: $(SRC)/fs_test.c
	$(CC) $(CFLAGS) -c $(SRC)/fs_test.c

fs_test: $(SRC)/fs_test.o	
	$(CC) $(CFLAGS) -o $(SRC)/fs_test $(SRC)/fs_test.o

run: $(SRC)/fs_test
	$(SRC)/fs_test

run_gdb: $(SRC)/fs_test
	gdb $(SRC)/fs_test

getxattr.o: $(SRC)/getxattr.c
	$(CC) $(CFLAGS) -c $(SRC)/getxattr.c

getxattr: $(SRC)/getxattr.o
	$(CC) $(CFLAGS) -o $(SRC)/getxattr $(SRC)/getxattr.o

clean:
	rm -f $(SRC)/fs_test.o $(SRC)/fs_test