# 编译器
CXX = g++

# 编译选项
CXXFLAGS = -std=c++17 -g $(INCLUDES)  # 将INCLUDES添加到CXXFLAGS

# 链接选项
LDFLAGS = -lfaiss -fopenmp -lopenblas -lspdlog -lrocksdb -lzstd -lpthread -ldl -llz4 -lsnappy -lbz2 -lz 

# Include directories
INCLUDES = -I ./hnswlib

# 目标文件
TARGET = vdb_server

# 源文件
SOURCES = vdb_server.cpp faiss_index.cpp http_server.cpp index_factory.cpp logger.cpp hnswlib_index.cpp scalar_storage.cpp vector_database.cpp

# 对象文件
OBJECTS = $(SOURCES:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(OBJECTS) -o $(TARGET) $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET)