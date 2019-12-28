/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"
#include <include/common/logger.h>

namespace cmudb {

/**
 * set ENABLE_LOGGING = true
 * 启动一个单独的线程来定期执行刷新到磁盘操作
 * 当log buffer满了或者缓冲区管理器想要强制刷新，flush能够被触发
 * （仅当被刷新页面的LSN大于persistent LSN时才发生）
 */
void LogManager::RunFlushThread() {
    /**
     * flush_buffer_用来将记录刷新到磁盘，log_buffer_用来并行的追加记录
     * 当出现下列三种情况时交换flush_buffer_和log_buffer_：
     *    1.When log_buffer is full
     *    2.When LOG_TIMEOUT is triggered
     *    3.When buffer pool is going to evict a dirty page from LRU replacer.
     */
    if (ENABLE_LOGGING) return;
    ENABLE_LOGGING = true;
    // 您需要启动一个单独的后台线程负责将日志刷新到磁盘文件中
    flush_thread_ = new thread([&] {
        // 线程触发每LOG_TIMEOUT秒或者log_buffer已满
        while (ENABLE_LOGGING) {
            unique_lock<mutex> latch(latch_);
            cv_.wait_for(latch, LOG_TIMEOUT, [&] { return needFlush_.load(); });
            assert(flushBufferSize_ == 0);
            if (logBufferOffset_ > 0) {
                swap(log_buffer_, flush_buffer_);
                swap(logBufferOffset_, flushBufferSize_);
                disk_manager_->WriteLog(flush_buffer_, flushBufferSize_);
                flushBufferSize_ = 0;
                SetPersistentLSN(lastLsn_);
            }
            needFlush_ = false;
            appendCv_.notify_all();
        }
    });
}
/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {
    if (!ENABLE_LOGGING) return;
    ENABLE_LOGGING = false;
    Flush(true);
    flush_thread_->join();
    assert(logBufferOffset_ == 0 && flushBufferSize_ == 0);
    delete flush_thread_;
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
    unique_lock<mutex> latch(latch_);
    if (logBufferOffset_ + log_record.GetSize() >= LOG_BUFFER_SIZE) {
        needFlush_ = true;
        cv_.notify_one();  // let RunFlushThread wake up.
        appendCv_.wait(latch, [&] {
            return logBufferOffset_ + log_record.GetSize() < LOG_BUFFER_SIZE;
        });
    }
    log_record.lsn_ = next_lsn_++;
    memcpy(log_buffer_ + logBufferOffset_, &log_record, LogRecord::HEADER_SIZE);
    int pos = logBufferOffset_ + LogRecord::HEADER_SIZE;

    if (log_record.log_record_type_ == LogRecordType::INSERT) {
        memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
        pos += sizeof(RID);
        // we have provided serialize function for tuple class
        log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
    } else if (log_record.log_record_type_ == LogRecordType::MARKDELETE ||
               log_record.log_record_type_ == LogRecordType::APPLYDELETE ||
               log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
        memcpy(log_buffer_ + pos, &log_record.delete_rid_, sizeof(RID));
        pos += sizeof(RID);
        log_record.delete_tuple_.SerializeTo(log_buffer_ + pos);
    } else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
        memcpy(log_buffer_ + pos, &log_record.update_rid_, sizeof(RID));
        pos += sizeof(RID);
        log_record.old_tuple_.SerializeTo(log_buffer_ + pos);
        pos += log_record.old_tuple_.GetLength() + sizeof(int32_t);
        log_record.new_tuple_.SerializeTo(log_buffer_ + pos);
    } else if (log_record.log_record_type_ == LogRecordType::NEWPAGE) {
        // prev_page_id
        memcpy(log_buffer_ + pos, &log_record.prev_page_id_, sizeof(page_id_t));
        pos += sizeof(page_id_t);
        memcpy(log_buffer_ + pos, &log_record.page_id_, sizeof(page_id_t));
    }
    logBufferOffset_ += log_record.GetSize();
    return lastLsn_ = log_record.lsn_;
}

void LogManager::Flush(bool force) {
    unique_lock<mutex> latch(latch_);
    if (force) {
        needFlush_ = true;
        cv_.notify_one();  // let RunFlushThread wake up.
        if (ENABLE_LOGGING)
            appendCv_.wait(latch, [&] {
                return !needFlush_.load();
            });  // block append thread
    } else {
        appendCv_.wait(latch);  // group commit,  But instead of forcing flush,
        // you need to wait for LOG_TIMEOUT or other operations to implicitly
        // trigger the flush operations
    }
}

}  // namespace cmudb
