/**
 * disk_manager.cpp
 */
#include <assert.h>
#include <cstring>
#include <iostream>
#include <sys/stat.h>
#include <thread>

#include "common/logger.h"
#include "disk/disk_manager.h"

namespace cmudb {


/**
 * 打开/创建一个数据库文件和日志文件
 * 输入参数为数据库的名字
 */
    DiskManager::DiskManager(const std::string &db_file)
            : file_name_(db_file), next_page_id_(0), num_flushes_(0), flush_log_(false),
              flush_log_f_(nullptr) {
        std::string::size_type n = file_name_.find(".");
        //没有在文件名字中发现“.”，文件格式不正确
        if (n == std::string::npos) {
            LOG_DEBUG("wrong file format");
            return;
        }
        log_name_ = file_name_.substr(0, n) + ".log";

        log_io_.open(log_name_,
                     std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
        // 如果打开或创建文件失败
        if (!log_io_.is_open()) {
            log_io_.clear();
            // 重新创建
            log_io_.open(log_name_, std::ios::binary | std::ios::trunc | std::ios::app |
                                    std::ios::out);
            log_io_.close();
            // 以原来的模式重新打开文件
            log_io_.open(log_name_, std::ios::binary | std::ios::in | std::ios::app |
                                    std::ios::out);
        }

        db_io_.open(db_file,
                    std::ios::binary | std::ios::in | std::ios::out | std::ios::out);
        if (!db_io_.is_open()) {
            db_io_.clear();
            db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
            db_io_.close();
            db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
        }
    }

    DiskManager::~DiskManager() {
        db_io_.close();
        log_io_.close();
    }

/**
 * 将指定页面的内容写入磁盘文件
 */
    void DiskManager::WritePage(page_id_t page_id, const char *page_data) {
        size_t offset = page_id * PAGE_SIZE;
        // 设置输出的起始位置
        db_io_.seekp(offset);
        //basic_ostream& write( const char_type* s, std::streamsize count )
        db_io_.write(page_data, PAGE_SIZE);
        if (db_io_.bad()) {
            LOG_DEBUG("I/O error while writing");
            return;
        }
        // 需要刷新以保持磁盘文件同步
        db_io_.flush();
    }

/**
 * 将指定页面的内容读入给定的存储区
 */
    void DiskManager::ReadPage(page_id_t page_id, char *page_data) {
        int offset = page_id * PAGE_SIZE;
        //检查给的page_id是否合法
        if (offset > GetFileSize(file_name_)) {
            LOG_DEBUG("I/O error while reading");
        } else {
            // 从offset位置开始读取
            db_io_.seekp(offset);
            db_io_.read(page_data, PAGE_SIZE);
            // 如果文件在读取PAGE_SIZE之前结束
            // std::streamsize gcount() const，返回最近的无格式输入操作所释出的字符数
            int read_count = db_io_.gcount();
            if (read_count < PAGE_SIZE) {
                LOG_DEBUG("Read less than a page");
                // 将页中剩余的位置全置为0
                memset(page_data + read_count, 0, PAGE_SIZE - read_count);
            }
        }
    }

/**
 * 将日志内容写入磁盘文件
 * 仅在同步完成后返回，并且仅执行顺序写入
 */
    void DiskManager::WriteLog(char *log_data, int size) {
        assert(log_data != buffer_used);
        buffer_used = log_data;
        //如果日志缓冲区为空，则对num_flushes_没有影响
        if (size == 0)
            return;

        flush_log_ = true;

        if (flush_log_f_ != nullptr)
            assert(flush_log_f_->wait_for(std::chrono::seconds(10)) ==
                   std::future_status::ready);

        num_flushes_ += 1;
        log_io_.write(log_data, size);

        if (log_io_.bad()) {
            LOG_DEBUG("I/O error while writing log");
            return;
        }
        // 需要刷新以保持磁盘文件同步
        log_io_.flush();
        flush_log_ = false;
    }

/**
 * 将日志内容读入给定的存储区
 * 始终从头开始读取并执行顺序读取
 * @return: false表示已经结束
 */
    bool DiskManager::ReadLog(char *log_data, int size, int offset) {
        if (offset >= GetFileSize(log_name_)) {
            // LOG_DEBUG("end of log file");
            // LOG_DEBUG("file size is %d", GetFileSize(log_name_));
            return false;
        }
        log_io_.seekp(offset);
        log_io_.read(log_data, size);
        // if log file ends before reading "size"
        int read_count = log_io_.gcount();
        if (read_count < size) {
            log_io_.clear();
            memset(log_data + read_count, 0, size - read_count);
        }

        return true;
    }

/**
 * 分配新页面，仅需增加计数器
 */
    page_id_t DiskManager::AllocatePage() { return next_page_id_++; }

/**
 * 回收页面
 */
    void DiskManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
        return;
    }

/**
 * 返回到目前为止的flush次数
 */
    int DiskManager::GetNumFlushes() const { return num_flushes_; }

/**
 * 如果当前正在清除日志，则返回true
 */
    bool DiskManager::GetFlushState() const { return flush_log_; }

/**
 * 获取磁盘文件大小
 */
    int DiskManager::GetFileSize(const std::string &file_name) {
        struct stat stat_buf;
        int rc = stat(file_name.c_str(), &stat_buf);
        return rc == 0 ? stat_buf.st_size : -1;
    }

} // namespace cmudb
