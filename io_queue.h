
#ifndef IO_QUEUE_H_
#define IO_QUEUE_H_

#include <iostream>
#include <queue>        // NOLINT
#include <mutex>        // NOLINT
#include <unistd.h>
#include <stdint.h>
#include <assert.h>

#include "async_io.h"

enum class IoEngine {
    IO_ENGINE_LIBAIO,
    IO_ENGINE_URING,
};

class Submitter {
public:
    Submitter(IoEngine ioEngine, unsigned ioDepth) {
        switch(ioEngine) {
            case IoEngine::IO_ENGINE_LIBAIO:
                ioChannel_ = new Libaio(ioDepth);
                break;
            case IoEngine::IO_ENGINE_URING:
                ioChannel_ = new Uring(ioDepth);
                break;
            default:
                assert(0);
        }
    }
    ~Submitter() {
        delete ioChannel_;
    }

     int Run() {
        if (pthread_create(&tidp_, nullptr, IoSubmitter, (void *)this)) {
            perror("failed to run submitter");
            return -1;
        }

        return 0;
    }

    void Finish() {
        pthread_cancel(tidp_);
        pthread_join(tidp_, nullptr);
    }

    void Push(IoTask *task){
        mtx_.lock();
        tasks_.push(task);
        mtx_.unlock();
    }
    
    AsyncIo *getIoChannel() {
        return ioChannel_;
    }

private:
    static void* IoSubmitter(void *arg) {
        Submitter *submitter = (Submitter *)arg;
        while(1) {
            pthread_testcancel();

            submitter->mtx_.lock();
            IoTask *task = submitter->tasks_.front();
            if (task == nullptr || submitter->ioChannel_->SubmitIo(task)) {
                submitter->mtx_.unlock();
                usleep(100);
                continue;
            }
            submitter->tasks_.pop();
            submitter->mtx_.unlock();
        }
    }

private:
    AsyncIo *ioChannel_;
    pthread_t tidp_;
    std::queue<IoTask *> tasks_;
    std::mutex mtx_;
};

class Reaper {
public:
    int Run(AsyncIo *ioChannel) {
        if(pthread_create(&tidp_, nullptr, CbHandler, ioChannel)) {
            perror("create reaper failed");
            return -1;
        }
        return 0;
    }
    void Finish() {
        pthread_cancel(tidp_);
        pthread_join(tidp_, nullptr);
    }

private:
    static void *CbHandler(void *arg) {
        AsyncIo *ioChannel = (AsyncIo *)arg;
        while (1) {
            pthread_testcancel();
            IoTask *task = ioChannel->ReapIo();
            if (task == nullptr) {
                usleep(100);
                continue;
            }
            if (task->cb != nullptr)
                task->cb(task);
        }
    }

    pthread_t tidp_;
};
#endif // IO_QUEUE_H_
