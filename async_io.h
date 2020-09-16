/* SPDX-License-Identifier: MIT */
/*
 * gcc -Wall -O2 -D_GNU_SOURCE -o io_uring-cp io_uring-cp.c -luring
 */

#ifndef ASYNC_IO_H_
#define ASYNC_IO_H_

#include <libaio.h>
#include <fcntl.h>

#ifdef ENABLE_URING
#include "liburing.h"
#endif

struct IoTask;
typedef void (*IocbFunc)(IoTask *task);
struct IoTask {
    int index; // thread index
    int fd;
    bool isRead;
    off_t first_offset, offset;
    size_t first_len;
    iovec iov;
    int res;
    IocbFunc cb;
    void *arg;
};

class AsyncIo {
public:
    virtual int SubmitIo(IoTask *task) { return 0; }
    virtual IoTask *ReapIo() { return nullptr; }
};

#ifdef ENABLE_URING
class Uring: public AsyncIo {
public:
    explicit Uring(unsigned ioDepth);
    ~Uring();
    int SubmitIo(IoTask *task);
    IoTask *ReapIo();

private:
    struct io_uring ring_;
};
#endif

class Libaio: public AsyncIo {
public:
    explicit Libaio(unsigned ioDepth);
    ~Libaio();
    int SubmitIo(IoTask *task);
    IoTask *ReapIo();

private:
    io_context_t ctx_;
};

#endif // ASYNC_IO_H_
