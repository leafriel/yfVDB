#ifndef NURAFT_LOG_VAL_TYPE_FORMATTER_H
#define NURAFT_LOG_VAL_TYPE_FORMATTER_H

#include <spdlog/fmt/bundled/core.h>
#include <libnuraft/log_val_type.hxx> // 确保包含正确的头文件

namespace fmt {

template <>
struct formatter<nuraft::log_val_type> {
    // 解析格式说明符，这里我们只实现一个简单的格式化器
    constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
        // 简单的实现，不解析任何格式说明符
        return ctx.begin();
    }

    // 执行格式化
    template <typename FormatContext>
    auto format(const nuraft::log_val_type& value, FormatContext& ctx) const 
        -> decltype(ctx.out()) {
        // 将 nuraft::log_val_type 转换为字符串
        std::string str;
        switch (value) {
            case nuraft::log_val_type::app_log:
                str = "app_log";
                break;
            case nuraft::log_val_type::conf:
                str = "conf";
                break;
            // 根据实际的枚举值添加更多的case
            default:
                str = "unknown";
                break;
        }
        return format_to(ctx.out(), "{}", str);
    }
};

} // namespace fmt

#endif // NURAFT_LOG_VAL_TYPE_FORMATTER_H
