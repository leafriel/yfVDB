#include "persistence.h"
#include "logger.h" 
#include "index_factory.h" // 包含 index_factory.h 以使用 IndexFactory 类
#include <rapidjson/document.h> // 包含 <rapidjson/document.h> 以使用 rapidjson::Document 类型
#include <rapidjson/writer.h> // 包含 rapidjson/writer.h 以使用 Writer 类
#include <rapidjson/stringbuffer.h> // 包含 rapidjson/stringbuffer.h 以使用 StringBuffer 类
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

Persistence::Persistence() : increaseID_(10), lastSnapshotID_(0) {} // 初始化 lastSnapshotID_ 成员变量

Persistence::~Persistence() {
    if (wal_log_file_.is_open()) {
        wal_log_file_.close();
    }
}

void Persistence::init(const std::string& local_path) {
    wal_log_file_.open(local_path, std::ios::in | std::ios::out | std::ios::app); // 以 std::ios::in | std::ios::out | std::ios::app 模式打开文件
    if (!wal_log_file_.is_open()) {
        GlobalLogger->error("An error occurred while writing the WAL log entry. Reason: {}", std::strerror(errno)); // 使用日志打印错误消息和原因
        throw std::runtime_error("Failed to open WAL log file at path: " + local_path);
    }

    loadLastSnapshotID();
}


uint64_t Persistence::increaseID() {
    increaseID_++;
    return increaseID_;
}

uint64_t Persistence::getID() const {
    return increaseID_;
}

void Persistence::writeWALLog(const std::string& operation_type, const rapidjson::Document& json_data, const std::string& version) { // 添加 version 参数
    uint64_t log_id = increaseID();

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    json_data.Accept(writer);

    wal_log_file_ << log_id << "|" << version << "|" << operation_type << "|" << buffer.GetString() << std::endl; // 将 version 添加到日志格式中

    if (wal_log_file_.fail()) { // 检查是否发生错误
        GlobalLogger->error("An error occurred while writing the WAL log entry. Reason: {}", std::strerror(errno)); // 使用日志打印错误消息和原因
    } else {
       GlobalLogger->debug("Wrote WAL log entry: log_id={}, version={}, operation_type={}, json_data_str={}", log_id, version, operation_type, buffer.GetString()); // 打印日志
       wal_log_file_.flush(); // 强制持久化
    }
}

void Persistence::writeWALRawLog(uint64_t log_id, const std::string& operation_type, const std::string& raw_data, const std::string& version) {
    wal_log_file_ << log_id << "|" << version << "|" << operation_type << "|" << raw_data << std::endl; // 将 version 添加到日志格式中

    if (wal_log_file_.fail()) { // 检查是否发生错误
        GlobalLogger->error("An error occurred while writing the WAL raw log entry. Reason: {}", std::strerror(errno)); // 使用日志打印错误消息和原因
    } else {
       GlobalLogger->debug("Wrote WAL raw log entry: log_id={}, version={}, operation_type={}, raw_data={}", log_id, version, operation_type, raw_data); // 打印日志
       wal_log_file_.flush(); // 强制持久化
    }
}

void Persistence::readNextWALLog(std::string* operation_type, rapidjson::Document* json_data) {
    GlobalLogger->debug("Reading next WAL log entry");

    std::string line;
    while (std::getline(wal_log_file_, line)) {
        std::istringstream iss(line);
        std::string log_id_str, version, json_data_str;

        std::getline(iss, log_id_str, '|');
        std::getline(iss, version, '|');
        std::getline(iss, *operation_type, '|'); // 使用指针参数返回 operation_type
        std::getline(iss, json_data_str, '|');

        uint64_t log_id = std::stoull(log_id_str); // 将 log_id_str 转换为 uint64_t 类型
        if (log_id > increaseID_) { // 如果 log_id 大于当前 increaseID_
            increaseID_ = log_id; // 更新 increaseID_
        }

        if (log_id > lastSnapshotID_){
            json_data->Parse(json_data_str.c_str()); // 使用指针参数返回 json_data
            GlobalLogger->debug("Read WAL log entry: log_id={}, operation_type={}, json_data_str={}", log_id_str, *operation_type, json_data_str);
            return;
        }else {
            GlobalLogger->debug("Skip Read WAL log entry: log_id={}, operation_type={}, json_data_str={}", log_id_str, *operation_type, json_data_str);
        }
    } 
    operation_type->clear();
    wal_log_file_.clear();
    GlobalLogger->debug("No more WAL log entries to read");

}

void Persistence::takeSnapshot(ScalarStorage& scalar_storage) { // 移除 takeSnapshot 方法的参数
    GlobalLogger->debug("Taking snapshot"); // 添加调试信息

    lastSnapshotID_ =  increaseID_;
    std::string snapshot_folder_path = "snapshots_";
    IndexFactory* index_factory = getGlobalIndexFactory(); // 通过全局指针获取 IndexFactory 实例
    index_factory->saveIndex(snapshot_folder_path, scalar_storage);

    saveLastSnapshotID();
}

void Persistence::loadSnapshot(ScalarStorage& scalar_storage) { // 添加 loadSnapshot 方法实现
    GlobalLogger->debug("Loading snapshot"); // 添加调试信息
    IndexFactory* index_factory = getGlobalIndexFactory();
    index_factory->loadIndex("snapshots_", scalar_storage); // 将 scalar_storage 传递给 loadIndex 方法
}

void Persistence::saveLastSnapshotID() { // 添加 saveLastSnapshotID 方法实现
    std::ofstream file("snapshots_MaxLogID");
    if (file.is_open()) {
        file << lastSnapshotID_;
        file.close();
    } else {
        GlobalLogger->error("Failed to open file snapshots_MaxID for writing");
    }
    GlobalLogger->debug("save snapshot Max log ID {}", lastSnapshotID_); // 添加调试信息
}

void Persistence::loadLastSnapshotID() { // 添加 loadLastSnapshotID 方法实现
    std::ifstream file("snapshots_MaxLogID");
    if (file.is_open()) {
        file >> lastSnapshotID_;
        file.close();
    } else {
        GlobalLogger->warn("Failed to open file snapshots_MaxID for reading");
    }
    
    GlobalLogger->debug("Loading snapshot Max log ID {}", lastSnapshotID_); // 添加调试信息

}