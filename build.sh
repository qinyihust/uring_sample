#!/bin/bash

g++ async_io.cpp io_demo.cpp -lpthread -laio -o demo -std=c++11
