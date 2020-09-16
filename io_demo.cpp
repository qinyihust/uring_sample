#include <iostream>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <cstdint>
#include <atomic>
#include "io_queue.h"

#define BS	(4*1024)
#define NR_THREAD 1
#define IO_NUM 10000000
#define IO_DEPTH 256

std::atomic<uint32_t> nr_flying_io{0};

struct TestEnv {
    int index;
    Submitter *submitter;
    int fd;
};

void *iobuf=nullptr;

int queueIo(TestEnv *env, off_t offset, off_t len, bool isRead, void *buf, IocbFunc cb, void *arg) {
    IoTask *task = (IoTask *)malloc(sizeof(*task));
    if (!task)
        return -1;

    task->index = env->index;
    task->fd = env->fd;

    task->isRead = isRead;
    task->offset = offset;
    task->first_offset = offset;
    task->first_len = len;
    task->iov.iov_base = buf;
    task->iov.iov_len = len;

    task->cb = cb;
    task->arg = env->submitter;
    task->res = -1;

    env->submitter->Push(task);

    return 0;
}

void cb1(IoTask *task) {
    assert (!task->isRead);
    unsigned num = ((char *)(task->iov.iov_base))[0];
    std::cout << "reaper: write for thread " << task->index <<" done, data="
              << std::hex << num << ", res="<< task->res << std::endl;

    delete task;
}


void cb0(IoTask *task) {
    assert (task->isRead);
    unsigned num = ((char *)(task->iov.iov_base))[0];
    std::cout << "reaper: read from thread " << task->index << " done, data="
              << std::hex << num << ", res=" << task->res << std::endl;
    num++;
    std::cout << "reaper: send write I/O for thread " << task->index << ", data="
	      << std::hex << num << std::endl;
    // send write I/O
    task->isRead = false;
    memset(task->iov.iov_base, num, BS);
    task->cb = cb0;
    task->res = -1;
    Submitter *submitter = (Submitter *)task->arg;
    submitter->Push(task);
}

void countdown(IoTask *task) {
    --nr_flying_io;
    delete task;
}

void *SendIo(void *arg) {
    TestEnv *env = (TestEnv *)arg;
    int idx = env->index;
    //std::cout << "thread " << env->index << ": read testfile" << std::endl;
        
    srand(time(NULL));
    int i = 0;
    for (i=0; i < IO_NUM; ++i) {
       while (nr_flying_io >= IO_DEPTH)
           usleep(100);
       
       ++nr_flying_io;
       off_t offset = ((off_t)rand() >> 7) << 12 ;    // max 67G
       //off_t offset = rand();
       //std::cout << "rand=" << offset << std::endl;
       //offset = offset >> 7;
       //std::cout << "reduce=" << offset << std::endl;
       //offset = offset << 12;
       //std::cout << "offset=" << offset << std::endl;
       queueIo(env, offset, BS, false, iobuf, countdown, nullptr);
    }

    //sleep(3);
    return nullptr;
}

int main(int argc, const char* argv[]) {
    int ret = 0;

#ifdef ENABLE_URING 
    if (argc != 2) {
	std::cout << "usage: " << argv[0] << " [option]" << std::endl;
	std::cout << "option: libaio or uring" << std::endl;
        return -1;
    }

    IoEngine engine = IoEngine::IO_ENGINE_NONE;
    if (!strncmp(argv[1], "libaio", 7))
	engine = IoEngine::IO_ENGINE_LIBAIO;
    else if (!strncmp(argv[1], "uring", 6))
	engine = IoEngine::IO_ENGINE_URING;
    else {
	std::cout << "usage: " << argv[0] << " [option]" << std::endl;
        std::cout << "option: libaio or uring" << std::endl;
        return -1;
    }
#else
    IoEngine engine = IoEngine::IO_ENGINE_LIBAIO;
#endif

    Submitter submitter(engine, IO_DEPTH);
    if (submitter.Run())
        return -1;

    Reaper reaper;
    if (reaper.Run(submitter.getIoChannel())) {
        submitter.Finish();
        return -1;
    }

    int fd = open("mnt/testfile", O_RDWR | O_CREAT | O_DIRECT, 0644);
    if (fd < 0) {
        perror("open file");
        return -1;
    }
    if (posix_memalign(&iobuf, getpagesize(), BS)) {
         std::cerr << "failed to alloc memory" << std::endl;
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
    fsync(fd);
    close(fd);

    return ret;
}
