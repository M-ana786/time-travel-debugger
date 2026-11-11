#pragma once

#include<string>
#include<cstdint>
#include<cstddef>
#include<cerrno>
#include<sys/socket.h>
using namespace std;


const int IO_BUFFER_SIZE = 64 * 1024;
const int MAX_VARS_PER_FRAME = 16;
const int MAX_STACK_DEPTH = 64;


const int MAX_NAME_LENGTH = 4096;  // longest name accepted when reading.tdbg
const int32_t MAX_STEPS = 1000;  //  a Snapshot is ~85 KB in RAM and there are no loops, but calls can fan out exponentially

struct Variable
{
    string name;
    int32_t value;
};

struct Frame
{
    string func_name;
    int32_t argc;
    Variable argv[MAX_VARS_PER_FRAME];
    int32_t returnLine;
    Variable locals[MAX_VARS_PER_FRAME];
    int32_t localCount;
};

struct Snapshot
{
    Frame callStack[MAX_STACK_DEPTH];
    int32_t stackDepth;
};

struct TTDBHeader
{
    char magic[4]; // "TTDB"
    int32_t version;
    int32_t stepCount;
    int64_t indexOffset;
};

// Helper.h should contain the structs, functions, constants, etc. that are used more than one time in the project. For example, the structs that are used in both server and client, the constants that are used in all three files.
struct FuncEntry
{
    string funcName;
    int64_t byteOffsetInResolveBin; // where this function's FUNC header record sits
};

struct PendingPatch
{
    int64_t byteOffsetOfOffsetField; // where in resolve.bin to seek back and overwrite
    string targetFuncName;
};


inline bool sendAll(int sock , const char *buf , size_t n){

    size_t sent = 0;

    while(sent < n){

        ssize_t r = send(sock , buf + sent , n - sent , MSG_NOSIGNAL);

        if(r < 0){

            if (errno == EINTR){

                continue;
            }
            return false;
        }

        if(r == 0){

            return false;
        }

        sent += static_cast<size_t>(r);
    }

    return true;
}

inline bool recvAll(int sock , char *buf , size_t n){

    size_t got = 0;

    while(got < n){

        ssize_t r = recv(sock , buf + got , n - got , 0);

        if(r < 0){

            if(errno == EINTR){

                continue;
            }
            return false;
        }

        if(r == 0){

            return false;
        }

        got += static_cast<size_t>(r);
    }


    return true;

}

struct CrcTable {

    uint32_t t[256];

    CrcTable(){

        for(uint32_t i = 0; i < 256; i++){

            uint32_t c = i;

            for(int j = 0; j < 8; j++){

                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }

            t[i] = c;
        }
    }

};

class Crc32{

    uint32_t crc;

public:
    Crc32(): crc(0xFFFFFFFFu){
    }

    void update(const char *data , size_t n){

        static const CrcTable tbl;

        for(size_t i = 0; i < n; i++){
            
            uint32_t byte = static_cast<uint8_t>(data[i]);
            uint32_t pos = (crc ^ byte) & 0xFFu;
            crc = tbl.t[pos] ^ (crc >> 8);
        }
    }

    uint32_t value() const {

        return crc ^ 0xFFFFFFFFu;
    }

};

inline void write_u32(char *out , uint32_t v){
    
    out[0] = static_cast<uint8_t>((v >> 24) & 0xFFu);
    out[1] = static_cast<uint8_t>((v >> 16) & 0xFFu);
    out[2] = static_cast<uint8_t>((v >> 8) & 0xFFu);
    out[3] = static_cast<uint8_t>(v & 0xFFu);
}

inline uint32_t read_u32(const char *in){
    
    uint32_t b0 = static_cast<uint8_t>(in[0]);
    uint32_t b1 = static_cast<uint8_t>(in[1]);
    uint32_t b2 = static_cast<uint8_t>(in[2]);
    uint32_t b3 = static_cast<uint8_t>(in[3]);

    return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
}

class TrailerChecker {
    
    Crc32 crc;
    char held[4];
    int heldCount;

public:
    explicit TrailerChecker(char status): heldCount(0){
    
        crc.update(&status , 1);
    }

    size_t feed(const char *in , size_t n , char *out){

        size_t outN = 0;

        for(size_t i = 0; i < n; i++){

            if(heldCount == 4){

                crc.update(&held[0] , 1);
                out[outN++] = held[0];
                held[0] = held[1];
                held[1] = held[2];
                held[2] = held[3];
                held[3] = in[i];

            }else{

                held[heldCount++] = in[i];
            }

        }

        return outN;
    }

    bool verify() const{

        if(heldCount < 4){

            return false;
        }

        return read_u32(held) == crc.value();
    }

};