
#ifndef IO_QUEUE_H_
#define IO_QUEUE_H_

#include <assert.h>
#include <iostream>
#include <mutex> // NOLINT
#include <queue> // NOLINT
#include <stdint.h>
#include <unistd.h>

#include "async_io.h"

enum class IoEngine {
  IO_ENGINE_LIBAIO,
  IO_ENGINE_URING,
  IO_ENGINE_NONE,
};

class Submitter {
public:
  Submitter(IoEngine ioEngine, unsigned ioDepth) {
    switch (ioEngine) {
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
  ~Submitter() { delete ioChannel_; }

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

  void Push(IoTask *task) {
    mtx_.lock();
    tasks_.push_back(task);
    mtx_.unlock();
  }

  AsyncIo *getIoChannel() { return ioChannel_; }

private:
  static void *IoSubmitter(void *arg) {
    Submitter *submitter = (Submitter *)arg;
    while (1) {
      pthread_testcancel();

      submitter->mtx_.lock();
      IoTask *task = submitter->tasks_.front();
      if (task == nullptr) {
        submitter->mtx_.unlock();
        usleep(100);
        continue;
      }
      submitter->tasks_.pop_front();
      submitter->mtx_.unlock();

      int ret = 0;
      if (ret = submitter->ioChannel_->SubmitIo(task)) {
        perror("submit failed");
        submitter->mtx_.lock();
        submitter->tasks_.push_front(task);
        submitter->mtx_.unlock();
        // wait and retry
        usleep(100);
        continue;
      }
    }
  }

private:
  AsyncIo *ioChannel_;
  pthread_t tidp_;
  std::deque<IoTask *> tasks_;
  std::mutex mtx_;
};

class Reaper {
public:
  int Run(AsyncIo *ioChannel) {
    if (pthread_create(&tidp_, nullptr, IoReaper, ioChannel)) {
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
  static void *IoReaper(void *arg) {
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

class CallbackWorker {
public:
  CallbackWorker(unsigned index, unsigned size) : index_(index), size_(size) {}
  ~CallbackWorker() {
    pthread_cancel(tid_);
    pthread_join(tid_, nullptr);
  }

  int Run() {
    if (pthread_create(&tid_, nullptr, CbWoker, (void *)this)) {
      perror("create callback worker failed");
      return -1;
    }

    return 0;
  }

  int Push(IoTask *task) {
    int ret = 0;
    lock_.lock();
    if (queue_.size() < size_) {
      queue_.push(task);
    } else {
      std::cout << " callback queue_ " << index_ << " is full" << std::endl;
      ret = -1;
    }
    lock_.unlock();

    return ret;
  }

private:
  static void *CbWoker(void *arg) {
    CallbackWorker *worker = (CallbackWorker *)arg;
    while (1) {
      pthread_testcancel();
      worker->lock_.lock();
      IoTask *task = worker->queue_.front();
      if (task == nullptr) {
        worker->lock_.unlock();
        usleep(100);
        continue;
      }
      worker->queue_.pop();
      worker->lock_.unlock();

      assert(task->cb != nullptr);
      task->cb(task);
    }
  }

private:
  unsigned index_;
  pthread_t tid_;
  std::queue<IoTask *> queue_;
  unsigned size_;
  std::mutex lock_;
};

enum class CallbackSchedule {
  CALLBACK_SCHED_ROUNDROBIN,
  CALLBACK_SCHED_HASH,
};

class CallbackPool {
public:
  CallbackPool(unsigned pool_size, unsigned queue_size, CallbackSchedule cb_sch)
      : poolSize_(pool_size), queueSize_(queue_size), cbSch_(cb_sch) {}
  ~CallbackPool() {
    for (unsigned i = 0; i < poolSize_; i++)
      delete workers_[i];
    delete workers_;
  }
  int Run() {
    workers_ = new CallbackWorker *[poolSize_];
    for (unsigned i = 0; i < poolSize_; i++) {
      workers_[i] = new CallbackWorker(i, queueSize_);
      assert(!workers_[i]->Run());
    }
  }
  void Push(IoTask *task) {
    static unsigned index = 0;
    bool done = false;
    // round robin
    for (; index < poolSize_; index++) {
      if (workers_[index]->Push(task))
        continue;
      done = true;
      std::cout << "task pushed to worker " << index << std::endl;
      break;
    }
    assert(done);
  }

private:
  unsigned poolSize_;
  unsigned queueSize_;
  CallbackSchedule cbSch_;
  CallbackWorker **workers_;
};

#endif // IO_QUEUE_H_
