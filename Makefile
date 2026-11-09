CC = gcc
CFLAGS = -Wall -Wextra -pthread

all: v1_echo_server v2_chat_server

v1_echo_server: v1_echo_server.c
	$(CC) $(CFLAGS) -o $@ $<

v2_chat_server: v2_chat_server.c
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f v1_echo_server v2_chat_server

.PHONY: all clean
