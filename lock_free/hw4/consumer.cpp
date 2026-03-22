#include "message_queue.h"
#include <iostream>
#include <thread>

int main() {
    MessageQueue mq(false);
    mq.Subscribe(10);
    while(true) {
        auto data = mq.Receive();
        if (data.has_value()) {
            std::cout << "Получено: " << (char*)data->data() << std::endl;
        } else {
            std::this_thread::yield();
        }
    }
    return 0;
}