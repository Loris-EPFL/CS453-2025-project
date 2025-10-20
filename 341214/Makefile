# Makefile for building the TL2 implementation shared library

CC := gcc
CXX := g++

# Output shared library (placed in parent directory)
BIN := ../341214.so

# Source files
SOURCES_CXX := tm.cpp

# Object files
OBJECTS := $(SOURCES_CXX:.cpp=.o)

# Compiler flags
CFLAGS := -std=c11 -fPIC -Wall -Wextra -Werror -O2 -g
CXXFLAGS := -std=c++17 -fPIC -Wall -Wextra -Werror -O2 -g

# Include directories
INCLUDES := -I../include

# Linker flags
LDFLAGS := -shared -lpthread

.PHONY: all clean

all: $(BIN)

$(BIN): $(OBJECTS)
	$(CXX) $(OBJECTS) -o $@ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f $(BIN) $(OBJECTS)