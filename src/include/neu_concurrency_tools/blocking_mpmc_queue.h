//
// Created by peng on 2021/7/1.
//

#ifndef NEUBLOCKCHAIN_BLOCKING_MPMC_QUEUE_H
#define NEUBLOCKCHAIN_BLOCKING_MPMC_QUEUE_H

#include "mpmc_queue.h"
#include "light_weight_semaphore.hpp"

template <typename T>
class BlockingMPMCQueue {
public:
    explicit BlockingMPMCQueue(size_t size = 100000): queue(size) {}

    template <typename P, typename = typename std::enable_if<std::is_nothrow_constructible<T, P &&>::value>::type>
    inline bool try_pop(P &&v) noexcept {
        if (sema.tryWait()) {
            while (!queue.try_pop(std::forward<P>(v)));
            return true;
        }
        return false;
    }

    inline bool try_dequeue(T &v) noexcept {
        if (sema.tryWait()) {
            while (!queue.try_pop(v));
            return true;
        }
        return false;
    }

    inline void wait_dequeue(T &v) noexcept {
        while (!sema.wait());
        queue.pop(v);
    }

    template <typename P, typename = typename std::enable_if<std::is_nothrow_constructible<T, P &&>::value>::type>
    inline void push(P &&v) noexcept {
        queue.push(std::forward<P>(v));
        sema.signal();
    }

    template <typename P, typename = typename std::enable_if<std::is_nothrow_constructible<T, P &&>::value>::type>
    inline bool enqueue(P &&v) noexcept {
        queue.push(std::forward<P>(v));
        sema.signal();
        return true;
    }

    std::size_t size_approx() noexcept{
        return sema.availableApprox();
    }

private:
    rigtorp::MPMCQueue<T> queue;
    moodycamel::LightweightSemaphore sema;
};

#endif //NEUBLOCKCHAIN_BLOCKING_MPMC_QUEUE_H
