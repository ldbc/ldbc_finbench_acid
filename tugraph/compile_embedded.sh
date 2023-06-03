INCLUDE_DIR=/usr/local/include
LIBLGRAPH=/usr/local/lib64/liblgraph.so
g++ -g -fopenmp -O3 -std=c++14 -I $INCLUDE_DIR -o $1 $1.cpp $LIBLGRAPH -lrt -fconcepts