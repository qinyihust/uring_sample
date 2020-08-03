#include <iostream>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include "io_queue.h"

#define QD	64
#define BS	(4*1024)
#define NR_THREAD 10

struct TestEnv {
    int index;
    Submitter *submitter;
    int fd;
};

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
    task->cb = cb1;
    task->res = -1;
    Submitter *submitter = (Submitter *)task->arg;
    submitter->Push(task);
}

void *SendIo(void *arg) {
    TestEnv *env = (TestEnv *)arg;
    int idx = env->index;
    char *data = nullptr;
    posix_memalign((void **)&data, getpagesize(), BS);
    memset(data, 0, BS);
    std::cout << "thread " << env->index << ": read testfile" << std::endl;
    queueIo(env, 0, BS, true, data, cb0, nullptr);

    sleep(3);
    return nullptr;
}

int main(int argc, const char* argv[]) {
    int ret = 0;

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
    	
    Submitter submitter(engine, QD);
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
    fsync(fd);
    close(fd);

    return ret;
}
