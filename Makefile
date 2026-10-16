CC = gcc
CFLAGS = -std=c99 -O3 -Wall -Wextra -Iinclude
LDFLAGS = 

SRC_DIR = src
INC_DIR = include
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = bench

TARGET = $(BUILD_DIR)/liblsm.a
TEST_TARGET = $(BUILD_DIR)/test_lsm
BENCH_TARGET = $(BUILD_DIR)/bench_lsm

SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRCS))

TEST_SRCS = $(wildcard $(TEST_DIR)/*.c)
BENCH_SRCS = $(wildcard $(BENCH_DIR)/*.c)

.PHONY: all build test clean benchmark directories

all: directories $(TARGET) $(TEST_TARGET) $(BENCH_TARGET)

directories:
	@mkdir -p $(BUILD_DIR)

build: directories $(TARGET)

$(TARGET): $(OBJS)
	ar rcs $@ $^

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

test: directories
	@if [ -d "$(TEST_DIR)" ] && [ -n "$$(ls -A $(TEST_DIR) 2>/dev/null)" ]; then \
		$(CC) $(CFLAGS) $(TEST_SRCS) $(TARGET) -o $(TEST_TARGET) $(LDFLAGS); \
		$(TEST_TARGET); \
	else \
		echo "No tests found in $(TEST_DIR). Skipping."; \
	fi

benchmark: directories
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$$(ls -A $(BENCH_DIR) 2>/dev/null)" ]; then \
		$(CC) $(CFLAGS) $(BENCH_SRCS) $(TARGET) -o $(BENCH_TARGET) $(LDFLAGS); \
		$(BENCH_TARGET); \
	else \
		echo "No benchmarks found in $(BENCH_DIR). Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)