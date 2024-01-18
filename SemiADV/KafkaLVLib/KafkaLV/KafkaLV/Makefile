# Linux Makefile
#
# Modify these to match your installation directories
#
# You can get rdkafka from https://github.com/confluentinc/librdkafka.git (tested with version 2.2.0)
# and then in the rdkafka directory, run ./configure and make
#
LVDIR ?= /usr/local/natinst/LabVIEW-2023-64/cintools
RDKAFKADIR ?= /usr/local/rdkafka

# You probably don't have to change anything below
CC = g++
# DEBUGFLAGS=-D_DEBUG
CFLAGS ?= -fPIC -std=c++17 -g3 -Wall $(DEBUGFLAGS)
INCLUDES = -I$(LVDIR) -I$(RDKAFKADIR)/src
LIBS = -Wl,--no-as-needed -L$(LVDIR) -L$(RDKAFKADIR)/src -L$(RDKAFKADIR)/src-cpp -luuid -lrdkafka -lrdkafka++
OBJS += Consumer.o KafkaLV.o Producer.o

all:	Linux/libKafkaLVLib.so

%.o:	%.cpp
	$(CC) $(CFLAGS) $(INCLUDES) -c -fpermissive $<

Linux/libKafkaLVLib.so:	$(OBJS)
	$(CC) -fPIC $(LIBS) -shared -o Linux/libKafkaLVLib.so $(OBJS)

clean:
	rm -f $(OBJS) libKafkaLVLib.so
