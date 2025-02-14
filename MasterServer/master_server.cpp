#include "master_server.h"
#include "logger.h"
#include <sstream>
#include <iostream>
#include <curl/curl.h>
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

ServerInfo ServerInfo::fromJson(const rapidjson::Document& value) {
    ServerInfo info;
    info.url = value["url"].GetString();
    info.role = static_cast<ServerRole>(value["role"].GetInt());
    return info;
}

rapidjson::Document ServerInfo::toJson() const {
    rapidjson::Document doc;
    doc.SetObject();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

    // 将 url 和 role 转换为 rapidjson::Value
    rapidjson::Value urlValue;
    urlValue.SetString(url.c_str(), allocator);

    rapidjson::Value roleValue;
    roleValue.SetInt(static_cast<int>(role));

    // 使用正确的类型添加成员
    doc.AddMember("url", urlValue, allocator);
    doc.AddMember("role", roleValue, allocator);

    return doc;
}


MasterServer::MasterServer(const std::string& etcdEndpoints, int httpPort)
: etcdClient_(etcdEndpoints), httpPort_(httpPort) {
    httpServer_.Get("/getNodeInfo", [this](const httplib::Request& req, httplib::Response& res) {
        getNodeInfo(req, res);
    });
    httpServer_.Post("/addNode", [this](const httplib::Request& req, httplib::Response& res) {
        addNode(req, res);
    });
    httpServer_.Delete("/removeNode", [this](const httplib::Request& req, httplib::Response& res) {
        removeNode(req, res);
    });
    httpServer_.Get("/getInstance", [this](const httplib::Request& req, httplib::Response& res) {
        getInstance(req, res);
    });

    // 启动定时器
    startNodeUpdateTimer();

}

void MasterServer::run() {
    httpServer_.listen("0.0.0.0", httpPort_);
}

void MasterServer::setResponse(httplib::Response& res, int retCode, const std::string& msg, const rapidjson::Document* data) {
    rapidjson::Document doc;
    doc.SetObject();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

    doc.AddMember("retCode", retCode, allocator);
    doc.AddMember("msg", rapidjson::Value(msg.c_str(), allocator), allocator);

    if (data != nullptr && data->IsObject()) {
        rapidjson::Value dataValue(rapidjson::kObjectType);
        dataValue.CopyFrom(*data, allocator); // 正确地复制 data
        doc.AddMember("data", dataValue, allocator);
    }

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);

    res.set_content(buffer.GetString(), "application/json");
}


void MasterServer::getNodeInfo(const httplib::Request& req, httplib::Response& res) {
    auto instanceId = req.get_param_value("instanceId");
    auto nodeId = req.get_param_value("nodeId");
    try {
        std::string etcdKey = "/instances/" + instanceId + "/nodes/" + nodeId;
        etcd::Response etcdResponse = etcdClient_.get(etcdKey).get();

        if (!etcdResponse.is_ok()) {
            setResponse(res, 1, "Error accessing etcd: " + etcdResponse.error_message());
            return;
        }

        rapidjson::Document doc;
        doc.SetObject();
        rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

        // 解析节点信息
        rapidjson::Document nodeDoc;
        nodeDoc.Parse(etcdResponse.value().as_string().c_str());
        if (!nodeDoc.IsObject()) {
            setResponse(res, 1, "Invalid JSON format");
            return;
        }

        // 构建响应
        doc.AddMember("instanceId", rapidjson::Value(instanceId.c_str(), allocator), allocator);
        doc.AddMember("nodeId", rapidjson::Value(nodeId.c_str(), allocator), allocator);
        doc.AddMember("nodeInfo", nodeDoc, allocator);
        
        setResponse(res, 0, "Node info retrieved successfully", &doc);
    } catch (const std::exception& e) {
        setResponse(res, 1, "Exception accessing etcd: " + std::string(e.what()));
    }
}


void MasterServer::addNode(const httplib::Request& req, httplib::Response& res) {
    rapidjson::Document doc;
    doc.Parse(req.body.c_str());
    if (!doc.IsObject()) {
        setResponse(res, 1, "Invalid JSON format");
        return;
    }

    try {
        std::string instanceId = doc["instanceId"].GetString();
        std::string nodeId = doc["nodeId"].GetString();
        std::string etcdKey = "/instances/" + instanceId + "/nodes/" + nodeId;

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);

        etcdClient_.set(etcdKey, buffer.GetString()).get();
        setResponse(res, 0, "Node added successfully");
    } catch (const std::exception& e) {
        setResponse(res, 1, std::string("Error accessing etcd: ") + e.what());
    }
}

void MasterServer::removeNode(const httplib::Request& req, httplib::Response& res) {
    auto instanceId = req.get_param_value("instanceId");
    auto nodeId = req.get_param_value("nodeId");
    std::string etcdKey = "/instances/" + instanceId + "/nodes/" + nodeId;

    try {
        etcd::Response etcdResponse = etcdClient_.rm(etcdKey).get();
        if (!etcdResponse.is_ok()) {
            setResponse(res, 1, "Error removing node from etcd: " + etcdResponse.error_message());
            return;
        }
        setResponse(res, 0, "Node removed successfully");
    } catch (const std::exception& e) {
        setResponse(res, 1, "Exception accessing etcd: " + std::string(e.what()));
    }
}

void MasterServer::getInstance(const httplib::Request& req, httplib::Response& res) {
    auto instanceId = req.get_param_value("instanceId");
    GlobalLogger->info("Getting instance information for instanceId: {}", instanceId);

    try {
        std::string etcdKeyPrefix = "/instances/" + instanceId + "/nodes/";
        GlobalLogger->debug("etcd key prefix: {}", etcdKeyPrefix);

        etcd::Response etcdResponse = etcdClient_.ls(etcdKeyPrefix).get();
        GlobalLogger->debug("etcd ls response received");

        if (!etcdResponse.is_ok()) {
            GlobalLogger->error("Error accessing etcd: {}", etcdResponse.error_message());
            setResponse(res, 1, "Error accessing etcd: " + etcdResponse.error_message());
            return;
        }

        const auto& keys = etcdResponse.keys();
        const auto& values = etcdResponse.values();
        
        rapidjson::Document doc;
        doc.SetObject();
        rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();
        
        rapidjson::Value nodesArray(rapidjson::kArrayType);
        for (size_t i = 0; i < keys.size(); ++i) {
            GlobalLogger->debug("Processing key: {}", keys[i]);
            rapidjson::Document nodeDoc;
            nodeDoc.Parse(values[i].as_string().c_str());
            if (!nodeDoc.IsObject()) {
                GlobalLogger->warn("Invalid JSON format for key: {}", keys[i]);
                continue;
            }

            // 使用 CopyFrom 方法将节点信息添加到数组中
            rapidjson::Value nodeValue(nodeDoc, allocator);
            nodesArray.PushBack(nodeValue, allocator);
        }

        doc.AddMember("instanceId", rapidjson::Value(instanceId.c_str(), allocator), allocator);
        doc.AddMember("nodes", nodesArray, allocator);
        
        GlobalLogger->info("Instance info retrieved successfully for instanceId: {}", instanceId);
        setResponse(res, 0, "Instance info retrieved successfully", &doc);
    } catch (const std::exception& e) {
        GlobalLogger->error("Exception accessing etcd: {}", e.what());
        setResponse(res, 1, "Exception accessing etcd: " + std::string(e.what()));
    }
}

void MasterServer::startNodeUpdateTimer() {
    std::thread([this]() {
        while (true) { // 这里可能需要一种更优雅的退出机制
            std::this_thread::sleep_for(std::chrono::seconds(10)); // 每10秒更新一次
            updateNodeStates();
        }
    }).detach();
}

size_t MasterServer::writeCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

void MasterServer::updateNodeStates() {
    CURL* curl = curl_easy_init();

    if (!curl) {
        GlobalLogger->error("CURL initialization failed");
        return;
    }

    try {
        std::string nodesKeyPrefix = "/instances/";
        GlobalLogger->info("Fetching nodes list from etcd");
        etcd::Response etcdResponse = etcdClient_.ls(nodesKeyPrefix).get();

        for (size_t i = 0; i < etcdResponse.keys().size(); ++i) {
            const std::string& nodeKey = etcdResponse.keys()[i];
            const std::string& nodeValue = etcdResponse.values()[i].as_string();

            rapidjson::Document nodeDoc;
            nodeDoc.Parse(nodeValue.c_str());
            if (!nodeDoc.IsObject()) {
                GlobalLogger->warn("Invalid JSON format for node: {}", nodeKey);
                continue;
            }

            std::string getNodeUrl = std::string(nodeDoc["url"].GetString()) + "/admin/getNode";
            GlobalLogger->debug("Sending request to {}", getNodeUrl);

            curl_easy_setopt(curl, CURLOPT_URL, getNodeUrl.c_str());
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);

            std::string responseStr;
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseStr);

            CURLcode res = curl_easy_perform(curl);
            bool needsUpdate = false;

            if (res != CURLE_OK) {
                GlobalLogger->error("curl_easy_perform() failed: {}", curl_easy_strerror(res));
                nodeErrorCounts[nodeKey]++;
                if (nodeErrorCounts[nodeKey] >= 5 && nodeDoc["status"].GetInt() != 0) {
                    nodeDoc["status"].SetInt(0); // Set status to 0 (abnormal)
                    needsUpdate = true;
                }
            } else {
                nodeErrorCounts[nodeKey] = 0; // Reset error count
                if (nodeDoc["status"].GetInt() != 1) {
                    nodeDoc["status"].SetInt(1); // Set status to 1 (normal)
                    needsUpdate = true;
                }

                rapidjson::Document getNodeResponse;
                getNodeResponse.Parse(responseStr.c_str());
                if (getNodeResponse.HasMember("node") && getNodeResponse["node"].IsObject()) {
                    std::string state = getNodeResponse["node"]["state"].GetString();
                    int newRole = (state == "leader") ? 0 : 1;

                    if (nodeDoc["role"].GetInt() != newRole) {
                        nodeDoc["role"].SetInt(newRole); // Update role
                        needsUpdate = true;
                    }
                }
            }

            if (needsUpdate) {
                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                nodeDoc.Accept(writer);

                etcdClient_.set(nodeKey, buffer.GetString()).get();
                GlobalLogger->info("Updated node {} with new status and role", nodeKey);
            }
        }
    } catch (const std::exception& e) {
        GlobalLogger->error("Exception while updating node states: {}", e.what());
    }

    curl_easy_cleanup(curl);
}











