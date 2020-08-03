#include "async_io.h"
#include <libaio.h>
#include <memory.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>

Uring::Uring(unsigned ioDepth) {
    assert(!io_uring_queue_init(ioDepth, &ring_, 0));
}

Uring::~Uring() {
    io_uring_queue_exit(&ring_);
}

int Uring::SubmitIo(IoTask *task) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr)
        return -1;
    if (task->isRead)
        io_uring_prep_readv(sqe, task->fd, &task->iov, 1, task->offset);
    else
        io_uring_prep_writev(sqe, task->fd, &task->iov, 1, task->offset);
    io_uring_sqe_set_data(sqe, task);
    io_uring_submit(&ring_);

    return 0;
}

IoTask *Uring::ReapIo() {
    struct io_uring_cqe *cqe = nullptr;
    io_uring_peek_cqe(&ring_ ,&cqe);
    if (!cqe)
        return nullptr;
    IoTask *task = (IoTask *)io_uring_cqe_get_data(cqe);
    assert(task != nullptr);
    task->res = cqe->res;
    io_uring_cqe_seen(&ring_, cqe);

    return task;
}

Libaio::Libaio(unsigned ioDepth) {
    assert(!io_setup(ioDepth, &ctx_));
}

Libaio::~Libaio() {
    io_destroy(ctx_);
}

int Libaio::SubmitIo(IoTask *task) {
    iocb *iocbp = new(iocb);
    if (task->isRead)
        io_prep_preadv(iocbp, task->fd, &(task->iov), 1, task->offset);
    else
        io_prep_pwritev(iocbp, task->fd, &(task->iov), 1, task->offset);
    iocbp->data = task;

    if (io_submit(ctx_, 1, &iocbp) < 0) {
        perror("failed to submit libaio");
        return -1;
    }

    return 0;
}

IoTask *Libaio::ReapIo() {
    io_event event;
    timespec time = {0, 10000}; //10us
    if (0 == io_getevents(ctx_, 1, 1, &event, &time))
        return nullptr;
    IoTask *task = (IoTask *)event.data;
    iocb *iocbp = event.obj;
    assert(task == iocbp->data);
    delete iocbp;
    task->res = event.res; // res: +x for done length, -x for error code

    return task;
}
