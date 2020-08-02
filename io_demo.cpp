#include <iostream>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include "io_queue.h"

#define QD	64
#define BS	(4*1024)
#define NR_THREAD 2

struct TestEnv {
    int index;
    Submitter *submitter;
    int fd;
};

int queueIo(TestEnv *env, off_t offset, off_t len, bool isRead, void *buf, IocbFunc cb, void *arg) {
    IoTask *task = (IoTask *)malloc(sizeof(*task));
    if (!task)
        return -1;

    task->fd = env->fd;

    task->isRead = isRead;
    task->offset = offset;
    task->first_offset = offset;
    task->first_len = len;
    task->iov.iov_base = buf;
    task->iov.iov_len = len;

    task->cb = cb;
    task->arg = arg;
    task->res = -1;

    env->submitter->Push(task);

    return 0;
}

void cb1(IoTask *task) {
    assert(task->isRead);
    std::cout << "read data: " << (static_cast<char *>(task->iov.iov_base))[0]
              << std::endl;

    delete task;
}

void *SendIo(void *arg) {
    TestEnv *env = (TestEnv *)arg;
    int idx = env->index;
    int fd = env->fd;

    std::string data(BS, 0);
    switch (idx) {
        case 0:
            data.assign(BS, '1');
            queueIo(env, 0, BS, false, (void *)(data.c_str()), nullptr, nullptr);
        case 1:
            sleep(10);
            queueIo(env, 0, BS, true, (void *)data.c_str(), cb1, nullptr);
    }
    sleep(20);

    return nullptr;
}


int main(int argc, const char* argv[]) {
    int ret = 0;
    Submitter submitter(IoEngine::IO_ENGINE_URING, QD);
    if (submitter.Run())
        return -1;

    Reaper reaper;
    if (reaper.Run(submitter.getIoChannel())) {
        submitter.Finish();
        return -1;
    }

    int fd = open("testfile", O_RDWR | O_CREAT | O_DIRECT, 0644);
    if (fd < 0) {
        perror("open file");
        return -1;
    }

    pthread_t tidp[NR_THREAD];
    TestEnv TestEnv[NR_THREAD];
    for (int i = 0; i < NR_THREAD; i++) {
        TestEnv[i].index = i;
        TestEnv[i].submitter = &submitter;
        TestEnv[i].fd = fd;
        if (pthread_create(&(tidp[i]), nullptr, SendIo, &(TestEnv[i]))) {
            std::cerr << "failed to create thread " << i << ", " 
                      << strerror(errno);
            ret = -1;
            goto out;
        }
    }

out:
    for (int i = 0; i < NR_THREAD; i++)
        pthread_join(tidp[i], nullptr);
    reaper.Finish();
    submitter.Finish();

    return ret;
}
