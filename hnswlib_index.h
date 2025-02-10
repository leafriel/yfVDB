#pragma once

#include "hnswlib/hnswlib.h"
#include "index_factory.h"
#include "roaring/roaring.h" // 包含 roaring/roaring.h 以使用 Roaring Bitmaps
#include <vector>

class HNSWLibIndex {
public:
    HNSWLibIndex(int dim, int num_data, IndexFactory::MetricType metric, int M = 16, int ef_construction = 200);
    void insert_vectors(const std::vector<float>& data, uint64_t label);
std::pair<std::vector<long>, std::vector<float>> search_vectors(const std::vector<float>& query, int k, const roaring_bitmap_t* bitmap = nullptr, int ef_search = 50);
    // 定义 RoaringBitmapIDFilter 类
    class RoaringBitmapIDFilter : public hnswlib::BaseFilterFunctor {
    public:
        RoaringBitmapIDFilter(const roaring_bitmap_t* bitmap) : bitmap_(bitmap) {}

        bool operator()(hnswlib::labeltype label) {
            return roaring_bitmap_contains(bitmap_, static_cast<uint32_t>(label));
        }

    private:
        const roaring_bitmap_t* bitmap_;
    };

private:
    hnswlib::HierarchicalNSW<float>* index;
};