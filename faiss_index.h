#pragma once

#include <faiss/Index.h>
#include <vector>

class FaissIndex {
public:
    FaissIndex(faiss::Index* index); 
    void insert_vectors(const std::vector<float>& data, uint64_t label);
    void remove_vectors(const std::vector<long>& ids); // 添加remove_vectors接口
    std::pair<std::vector<long>, std::vector<float>> search_vectors(const std::vector<float>& query, int k);

private:
    faiss::Index* index;
};