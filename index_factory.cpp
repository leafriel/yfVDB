#include "index_factory.h"
#include "hnswlib_index.h"
#include "filter_index.h" // 包含 filter_index.h 以使用 FilterIndex 类

#include <faiss/IndexFlat.h>
#include <faiss/IndexIDMap.h>

namespace {
    IndexFactory globalIndexFactory; 
}

IndexFactory* getGlobalIndexFactory() {
    return &globalIndexFactory; 
}

void IndexFactory::init(IndexFactory::IndexType type, int dim, int num_data, IndexFactory::MetricType metric) {
    faiss::MetricType faiss_metric = (metric == IndexFactory::MetricType::L2) ? faiss::METRIC_L2 : faiss::METRIC_INNER_PRODUCT;

    switch (type) {
        case IndexFactory::IndexType::FLAT:
            index_map[type] = new FaissIndex(new faiss::IndexIDMap(new faiss::IndexFlat(dim, faiss_metric)));
            break;
        case IndexFactory::IndexType::HNSW:
            index_map[type] = new HNSWLibIndex(dim, num_data, metric, 16, 200);
            break;
        case IndexFactory::IndexType::FILTER: // 初始化 FilterIndex 对象
            index_map[type] = new FilterIndex();
            break;
        default:
            break;
    }
}

void* IndexFactory::getIndex(IndexType type) const { 
    auto it = index_map.find(type);
    if (it != index_map.end()) {
        return it->second;
    }
    return nullptr;
}