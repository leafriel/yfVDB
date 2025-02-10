#include "filter_index.h"
#include "logger.h" // 包含 logger.h 以使用日志记录器
#include <algorithm>
#include <set>
#include <memory>

FilterIndex::FilterIndex() {}

void FilterIndex::addIntFieldFilter(const std::string& fieldname, int64_t value, uint64_t id) {
    roaring_bitmap_t* bitmap = roaring_bitmap_create();
    roaring_bitmap_add(bitmap, id);
    intFieldFilter[fieldname][value] = bitmap;
    GlobalLogger->debug("Added int field filter: fieldname={}, value={}, id={}", fieldname, value, id); // 添加打印信息

}

void FilterIndex::updateIntFieldFilter(const std::string& fieldname, int64_t* old_value, int64_t new_value, uint64_t id) {// 将 old_value 参数更改为指针类型
    if (old_value != nullptr) {
        GlobalLogger->debug("Updated int field filter: fieldname={}, old_value={}, new_value={}, id={}", fieldname, *old_value, new_value, id);
    } else {
        GlobalLogger->debug("Updated int field filter: fieldname={}, old_value=nullptr, new_value={}, id={}", fieldname, new_value, id);
    }    
    
    auto it = intFieldFilter.find(fieldname);
    if (it != intFieldFilter.end()) {
        std::map<long, roaring_bitmap_t*>& value_map = it->second;

        // 查找旧值对应的位图，并从位图中删除 ID
        auto old_bitmap_it = (old_value != nullptr) ? value_map.find(*old_value) : value_map.end(); // 使用解引用的 old_value
        if (old_bitmap_it != value_map.end()) {
            roaring_bitmap_t* old_bitmap = old_bitmap_it->second;
            roaring_bitmap_remove(old_bitmap, id);
        }

        // 查找新值对应的位图，如果不存在则创建一个新的位图
        auto new_bitmap_it = value_map.find(new_value);
        if (new_bitmap_it == value_map.end()) {
            roaring_bitmap_t* new_bitmap = roaring_bitmap_create();
            value_map[new_value] = new_bitmap;
            new_bitmap_it = value_map.find(new_value);
        }

        roaring_bitmap_t* new_bitmap = new_bitmap_it->second;
        roaring_bitmap_add(new_bitmap, id);
    } else {
        addIntFieldFilter(fieldname, new_value, id);
    }
}

void FilterIndex::getIntFieldFilterBitmap(const std::string& fieldname, Operation op, int64_t value, roaring_bitmap_t* result_bitmap) { // 添加 result_bitmap 参数
    auto it = intFieldFilter.find(fieldname);
    if (it != intFieldFilter.end()) {
        auto& value_map = it->second;

        if (op == Operation::EQUAL) {
            auto bitmap_it = value_map.find(value);
            if (bitmap_it != value_map.end()) {
                GlobalLogger->debug("Retrieved EQUAL bitmap for fieldname={}, value={}", fieldname, value);
                roaring_bitmap_or_inplace(result_bitmap, bitmap_it->second); // 更新 result_bitmap
            }
        } else if (op == Operation::NOT_EQUAL) {
            for (const auto& entry : value_map) {
                if (entry.first != value) {
                    roaring_bitmap_or_inplace(result_bitmap, entry.second); // 更新 result_bitmap
                }
            }
            GlobalLogger->debug("Retrieved NOT_EQUAL bitmap for fieldname={}, value={}", fieldname, value);
        }
    }
}