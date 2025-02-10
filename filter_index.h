#pragma once

#include <vector>
#include <map>
#include <string>
#include <set>
#include <memory> // 包含 <memory> 以使用 std::shared_ptr
#include "roaring/roaring.h"

class FilterIndex {
public:
    enum class Operation {
        EQUAL,
        NOT_EQUAL
    };

    FilterIndex();
    void addIntFieldFilter(const std::string& fieldname, int64_t value, uint64_t id);
    void updateIntFieldFilter(const std::string& fieldname, int64_t* old_value, int64_t new_value, uint64_t id); // 将 old_value 参数更改为指针类型
    void getIntFieldFilterBitmap(const std::string& fieldname, Operation op, int64_t value, roaring_bitmap_t* result_bitmap); // 添加 result_bitmap 参数

private:
    std::map<std::string, std::map<long, roaring_bitmap_t*>> intFieldFilter;
};