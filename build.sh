#!/bin/bash

g++ async_io.cpp io_demo.cpp -lpthread -luring -laio -o demo
