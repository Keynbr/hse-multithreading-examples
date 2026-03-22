#pragma once
#include "protocol.h"
#include <vector>
#include <set>
#include <optional>

class MessageQueue {
public:
    MessageQueue(bool producer, size_t size = 0);
    ~MessageQueue();
    bool Send(uint32_t type, const void* data, uint32_t len);
    void Subscribe(uint32_t type);
    std::optional<std::vector<uint8_t>> Receive();

private:
    bool m_producer;
    size_t m_size;
    void* m_mmap_ptr;
    QueueData* m_data;
    uint8_t* m_buffer;
    std::set<uint32_t> m_filters;
};