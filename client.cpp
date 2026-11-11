// ======================= TIME-TRAVEL DEBUGGER - CLIENT TEMPLATE =======================

// The client never talks to the server directly. It only talks to the load balancer.
// The load balancer wakes the server up if it is down, so the client does not care.
// The first "start" may just take a bit longer.
//
// The network is used ONLY by "start". After the .tdbg file is downloaded,
// next / prv / jmp read that file on the local disk. They never contact the load balancer.
//
// How the pieces fit together:
//   main loop -> readCommand -> runCommand
//   start -> cmdStart()  -> requestDebugFile() (Part A) -> openTdbg() (Part B) -> shows step 1
//   next, prv, jmp -> moveBy() (Part D) -> readSnapshotAt() (Part B) -> printSnapshot() (Part E)

#include <iostream>
#include <string>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <signal.h>
#include <vector>
#include <sstream>
#include <sys/socket.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include "helper.h"
using namespace std;

// ---- Constants ----

const char *PROGRAM_NAME = "ttdb";
const char *PROMPT = "ttdb> ";
const char *TDBG_FILE = "ttdb_last.tdbg"; // where the downloaded .tdbg is saved
const int MAX_JUMP = 1000000000;          // biggest n accepted by jmp
const int CLIENT_TIMEOUT_SEC = 30;   // how long we wait for load balancer's reply
const size_t MAX_ERROR_SIZE = 4096;
const int64_t TDBG_HEADER_SIZE = 20; // 4+4+4+8 bytes

string LB_HOST = "127.0.0.1";
int LB_PORT = 5000;

// ---- Structs only the client needs ----

struct TdbgReader
{
    // ssize_t file_index;
    FILE *fp = nullptr;
    TTDBHeader header;
    vector<int64_t> index;
};

struct DebugSession
{
    bool active_flag;     // false until a "start" or open  has worked
    TdbgReader reader;   // the .tdbg file stays open for the whole session
    int64_t currentStep; // 0-based
};

// =============================================================================================
// PART A: TALKING TO THE LOAD BALANCER
// =============================================================================================

int connectToHost(const char *host, int port)
{
    if(port < 1 || port > 65535){

        return -1;
    }

    int sock = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

    if(sock < 0){

        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if(inet_pton(AF_INET, host, &addr.sin_addr) <= 0){

        close(sock);
        return -1;
    }

  
    timeval connectTimeout{};
    connectTimeout.tv_sec = 5;

    if(setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &connectTimeout, sizeof(connectTimeout)) != 0){

        close(sock);
        return -1;
    }

    if(connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0){

        close(sock);
        return -1;
    }

    return sock;
}

bool uploadSource(int sock, const char *sourcePath)
{
    // have to do checksum and then send
    // When the file is finished, call shutdown(sock, SHUT_WR). This is how the server knows the string is complete.
    // Return false if the file cannot be opened or any send fails.

    FILE *f = fopen(sourcePath, "rb");

    if(f == nullptr){

        cout << " Couldn't open file" << endl;
        return false;
    }

    Crc32 crc;

    char status = static_cast<char>(0);
    crc.update(&status , 1);

    if(!sendAll(sock, &status, 1)){

        fclose(f);
        return false;
    }

    vector<char> buf(IO_BUFFER_SIZE);

    while(true){

        size_t got = fread(buf.data(), 1, buf.size(), f);

        if(got > 0){

            crc.update(buf.data(), got);

            if(!sendAll(sock, buf.data(), got)){

                fclose(f);
                return false;
            }
        }

        if(got < buf.size()){

            break;
        }
    }

    bool readFailed = ferror(f) != 0;
    fclose(f);

    if(readFailed){

        return false;
    }

    char tail[4];
    write_u32(tail, crc.value());

    if(!sendAll(sock, tail, 4)){

        return false;
    }


    if(shutdown(sock, SHUT_WR) != 0){

        return false;
    }


    return true;
}

bool receiveResponse(int sock, const char *outTdbgPath, string &outError)
{
    // Reply on the wire: [status][payload][CRC-32]. The CRC covers the status byte too.
    // Feed every received byte (status included) to a TrailerChecker.
    // Status 1: collect the payload into outError. Status 0: write the payload to the file.
    // At the end call verify(). If it fails, delete the file, set outError to
    // "checksum mismatch, response corrupted" and return false.

    outError.clear();
   
    char status;

    if(!recvAll(sock, &status, 1)){
   
        outError = "no reply from load balancer (timed out or connection closed)";
        return false;
    }

    if(status != 0 && status != 1){
   
        outError = "Invalid reply from load balancer";
        return false;
    }

    TrailerChecker checker(status);

    FILE* f = nullptr;

    if(status == 0){
        f = fopen(outTdbgPath, "wb");

        if(f == nullptr){
   
            outError = "couldn't create the file for the download ";
            return false;
        }
   
    }

    vector<char> buf(IO_BUFFER_SIZE);
    vector<char> body(IO_BUFFER_SIZE);
    string failure;

    while(true){
        ssize_t r = recv(sock, buf.data(), buf.size(), 0);

        if(r < 0){
   
            if(errno == EINTR){
   
                continue;
            }

            if(errno == EAGAIN || errno == EWOULDBLOCK){
   
                failure = "timed out waiting for the reply";
            
            }else{
   
                failure = "connection error while recovering the reply";
            }

            break;
        }

        if(r == 0){
            
            break;
        }

        size_t released = checker.feed(buf.data(), static_cast<size_t>(r), body.data());

        if(status == 0){
            
            if(released > 0 && fwrite(body.data(), 1, released, f) != released){
            
                failure = "couldn't write the downloaded file";
                break;
            }

        }else{
            
            size_t remaining = MAX_ERROR_SIZE - outError.size();
            size_t tokeep = released;

            if(tokeep > remaining){
            
                tokeep = remaining;
            }

            outError.append(body.data(), tokeep);
        }

    }

    if(f != nullptr){
        
        if(fclose(f) != 0 && failure.empty()){
        
            failure = "failed to save the downloaded file";
        }
    }

    if(!failure.empty()){
        
        if(status == 0){
        
            remove(outTdbgPath);
        }

        outError = failure;
        return false;
    }

    if(!checker.verify()){
        
        if(status == 0){
        
            remove(outTdbgPath);
        }

        outError = "checksum  mismatch , response corrupted";
        return false;
    }

    if(status == 1){
        
        if(outError.empty()){
        
            outError = " the server reported an error ";
        }

        return false;
    }

    return true;

}
bool requestDebugFile(const string &host, int port, const string &sourcePath, const char *outTdbgPath, string &outError){
 
    outError.clear();

    FILE* probe = fopen(sourcePath.c_str(), "rb");

    if(probe == nullptr){
 
        outError = " couldn't open " + sourcePath;
        return false;
    }

    fclose(probe);

    int sock = connectToHost(host.c_str(), port);

    if(sock < 0){
 
        outError = " couldn't connect to load balancer at " + host + " : " + to_string(port);
        return false;
    }

    timeval tv{};
    tv.tv_sec = CLIENT_TIMEOUT_SEC;
    tv.tv_usec = 0;

    if(setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) != 0){
 
        outError = "couldn't set the recieve timeout";
        close(sock);
        return false;
    }

   
    if(setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) != 0){
 
        outError = "couldn't set the send timeout";
        close(sock);
        return false;
    }

    if(!uploadSource(sock, sourcePath.c_str())){
 
        string lbError;
        bool gotFile = receiveResponse(sock, outTdbgPath, lbError);
        close(sock);

        if(gotFile){
 
            remove(outTdbgPath);
        }

        if(!gotFile && !lbError.empty()){
 
            outError = lbError;
        }else{
 
            outError = "upload to the load balancer failed";
        }

        return false;
    }

    bool ok = receiveResponse(sock, outTdbgPath, outError);
    close(sock);
    return ok;
}
// =============================================================================================
// PART B: READING THE .tdbg FILE
// File layout (written by Phase 1):
//   header | snapshot 0 | snapshot 1 | ... | index (one 8 byte offset per snapshot)
// The index lets us jump straight to any step without reading the steps before it.
// =============================================================================================

void closeTdbg(TdbgReader &r);

bool openTdbg(const char *path, TdbgReader &r)
{
    const int32_t supportedVersion = 1;

    if(r.fp != nullptr){

        return false;
    }

    r.header = TTDBHeader{};
    r.index.clear();
    r.fp = fopen(path, "rb");

    if(r.fp == nullptr){

        cout << " couldn't open file" << endl;
        return false;
    }

    TTDBHeader &h = r.header;

    bool ok = fread(h.magic, 1, 4, r.fp) == 4 && fread(&h.version, sizeof(h.version), 1, r.fp) == 1 && fread(&h.stepCount, sizeof(h.stepCount), 1, r.fp) == 1 && fread(&h.indexOffset, sizeof(h.indexOffset), 1, r.fp) == 1;

    if(!ok){
    
        closeTdbg(r);
        return false;
    }

   
    if(memcmp(h.magic, "TTDB", 4) != 0 || h.version != supportedVersion || h.stepCount <= 0 || h.stepCount > MAX_STEPS){
      
        closeTdbg(r);
        return false;
    }

    if(fseek(r.fp, 0, SEEK_END) != 0){
      
        closeTdbg(r);
        return false;
    }

    long endpos = ftell(r.fp);

    if(endpos < 0){
      
        closeTdbg(r);
        return false;
    }

    
    int64_t filesize = static_cast<int64_t>(endpos);
    int64_t indxbyte = static_cast<int64_t>(h.stepCount) * sizeof(int64_t);

    if(filesize < TDBG_HEADER_SIZE || h.indexOffset < TDBG_HEADER_SIZE ||  h.indexOffset > filesize || indxbyte != filesize - h.indexOffset){
      
        closeTdbg(r);
        return false;
    }

    if(fseek(r.fp, static_cast<long>(h.indexOffset), SEEK_SET) != 0){
      
        closeTdbg(r);
        return false;
    }

    r.index.resize(static_cast<size_t>(h.stepCount));

    if(fread(r.index.data(), sizeof(int64_t), r.index.size(), r.fp) != r.index.size()){
      
        closeTdbg(r);
        return false;
    }

    
    if(r.index[0] != TDBG_HEADER_SIZE){
      
        closeTdbg(r);
        return false;
    }

    int64_t previous = TDBG_HEADER_SIZE - 1;
    for(size_t i = 0; i < r.index.size(); i++){
      
        if(r.index[i] <= previous || r.index[i] >= h.indexOffset){
      
            closeTdbg(r);
            return false;
        }

        previous = r.index[i];
    }

    return true;

}

void closeTdbg(TdbgReader &r)
{
    // Free r.index and close the file.

    if(r.fp != nullptr){

        fclose(r.fp);
        r.fp = nullptr;
    }

    r.header = TTDBHeader{};
    r.index.clear();

}
bool readInt32(FILE *f, int32_t &v){
 
    return fread(&v, sizeof(int32_t), 1, f) == 1;
}

bool readstring(FILE* f, string &out){
 
    int32_t len;

    if(!readInt32(f, len)){
 
        return false;
    }

    if(len < 0 || len > MAX_NAME_LENGTH){
 
        return false;
    }

    out.resize(static_cast<size_t>(len));

    if(len > 0 && fread(&out[0], 1, static_cast<size_t>(len), f) != static_cast<size_t>(len)){
 
        return false;
    }

    return true;

}

bool readVariable(FILE* f, Variable &v){

    return readstring(f, v.name) && readInt32(f, v.value);
}

bool readFrame(FILE* f, Frame &fr){

    if(!readstring(f, fr.func_name)){

        return false;
    }

    if(!readInt32(f, fr.argc) || fr.argc < 0 || fr.argc > MAX_VARS_PER_FRAME){

        return false;
    }

    for(int32_t i = 0; i < fr.argc; i++){

        if(!readVariable(f, fr.argv[i])){

            return false;
        }
    }

    if(!readInt32(f, fr.returnLine)){

        return false;
    }

    if(!readInt32(f, fr.localCount) || fr.localCount < 0 || fr.localCount > MAX_VARS_PER_FRAME){

        return false;
    }

    for(int32_t i = 0; i < fr.localCount; i++){

        if(!readVariable(f, fr.locals[i])){

            return false;
        }
    }

    return true;
}

Snapshot *readSnapshotAt(TdbgReader &r, int32_t step)
{

    if(r.fp == nullptr || step < 0 || step >= r.header.stepCount){

        return nullptr;
    }

    if(fseek(r.fp, static_cast<long>(r.index[static_cast<size_t>(step)]), SEEK_SET) != 0){

        return nullptr;
    }

    Snapshot *snap = new Snapshot();

    int32_t depth;

    if(!readInt32(r.fp, depth) || depth < 0 || depth > MAX_STACK_DEPTH){

        delete snap;
        return nullptr;
    }

    snap->stackDepth = depth;

    for(int32_t i = 0; i < depth; i++){

        if(!readFrame(r.fp, snap->callStack[i])){

            delete snap;
            return nullptr;
        }
    }

  
    int64_t expectedEnd = r.header.indexOffset;

    if(step + 1 < r.header.stepCount){

        expectedEnd = r.index[static_cast<size_t>(step + 1)];
    }

    long actualEnd = ftell(r.fp);

    if(actualEnd < 0 || static_cast<int64_t>(actualEnd) != expectedEnd){

        delete snap;
        return nullptr;
    }

    return snap;

}

// =============================================================================================
// PART C: THE SESSION IN MEMORY
// There is one DebugSession variable, created in main() and passed to every command by reference.
// =============================================================================================

void initSession(DebugSession &s)
{

    s.active_flag = false;
    s.reader.fp = nullptr;
    s.reader.header = TTDBHeader{};
    s.reader.index.clear();
    s.currentStep = 0;
}

void closeSession(DebugSession &s)
{
    // If active, call closeTdbg(s.reader). Then set active = false.
    // Called when the user types quit and when the input ends.

    if(s.active_flag){

        closeTdbg(s.reader);
    }

    s.active_flag = false;

}

// =============================================================================================
// PART D: THE COMMANDS
// =============================================================================================

void printSnapshot(int32_t step, int32_t totalSteps, const Snapshot &s); // defined in Part E

bool showCurrent(DebugSession &s)
{
    // Read the snapshot at s.currentStep, call printSnapshot, delete the snapshot.
    // If readSnapshotAt returns nullptr, print "damaged .tdbg file" and return false.

    if(!s.active_flag){

        cout << " No session yet  Use start<source file>" << endl;
        return false;
    }

    Snapshot *snap = readSnapshotAt(s.reader, static_cast<int32_t>(s.currentStep));


    if(snap == nullptr){

        cout << " ERROR: damaged .tdbg file " << endl;
        return false;
    }

    printSnapshot(static_cast<int32_t>(s.currentStep) + 1, s.reader.header.stepCount, *snap);
    delete snap;
    return true;

}
bool moveBy(DebugSession &s, int32_t delta)
{

    if(!s.active_flag){

        cout << " No session yet  Use start<source file>" << endl;
        return false;
    }

    int64_t total = s.reader.header.stepCount;
    int64_t target = s.currentStep + delta;

    if(target < 0){

        if(s.currentStep == 0){

            cout << " Already at first step " << endl;
        }else{

            cout << " Can't go back that far. You are at step " << (s.currentStep + 1) << " of " << total << endl;
        }

        return false;
    }

    if(target >= total){

        if(s.currentStep == total - 1){

            cout << "Already at last step " << endl;
        }else{

            cout << " Can't go forward that far. You are at step " << (s.currentStep + 1) << " of " << total << endl;
        }

        return false;
    }

    int64_t previous = s.currentStep;
    s.currentStep = target;

    if(!showCurrent(s)){

        s.currentStep = previous;
        return false;
    }

    return true;

}

bool cmdStart(DebugSession &s, const string &sourcePath)
{
    // 1. requestDebugFile with host and port, saving to TDBG_FILE.
    // 2. If it fails, print the error text and return false. Keep the old session as it was.
    // 3. Open the new file with openTdbg into a NEW local TdbgReader.
    //    If that fails, print "server sent a damaged .tdbg file", keep the old session, return false.
    // 4. Only now close the old reader (if s.active), then put the new reader into s,
    //    set s.currentStep = 0 and s.active = true.
    // 5. Show step 1 with showCurrent(s).

   
    string tmpPath = string(TDBG_FILE) + "." + to_string(getpid()) + ".new";
    string error;

    cout << "Sending " << sourcePath << " ..." << endl;

    if(!requestDebugFile(LB_HOST, LB_PORT, sourcePath, tmpPath.c_str(), error)){

        remove(tmpPath.c_str());
        cout << "Error: " << error << endl;
        return false;
    }

    TdbgReader fresh;
    fresh.fp = nullptr;

    if(!openTdbg(tmpPath.c_str(), fresh)){

        remove(tmpPath.c_str());
        cout << " Error: server sent a damaged .tdbg file " << endl;
        return false;
    }

   
    for(int32_t i = 0; i < fresh.header.stepCount; i++){

        Snapshot* snap = readSnapshotAt(fresh, i);

        if(snap == nullptr){

            cout << " Error: server sent a damaged .tdbg file " << endl;
            closeTdbg(fresh);
            remove(tmpPath.c_str());
            return false;
        }

        delete snap;
    }

    if(rename(tmpPath.c_str(), TDBG_FILE) != 0){

        cout << " Error: couldn't save the download as " << TDBG_FILE << endl;
        closeTdbg(fresh);
        remove(tmpPath.c_str());
        return false;

    }

    if(s.active_flag){

        closeTdbg(s.reader);
    }

   
    s.reader.fp = fresh.fp;
    s.reader.header = fresh.header;
    s.reader.index.swap(fresh.index);
    fresh.fp = nullptr;

    s.currentStep = 0;
    s.active_flag = true;

    return showCurrent(s);

}

bool cmdNext(DebugSession &s){

    return moveBy(s, 1);
}

bool cmdPrv(DebugSession &s){

    return moveBy(s, -1);
}

bool cmdJmpNext(DebugSession &s, int32_t n){

    if(n < 1 || n > MAX_JUMP){

        cout << " Error : the jump must be from 1 to " << MAX_JUMP << endl;
        return false;
    }

    return moveBy(s, n);

}

bool cmdJmpPrv(DebugSession &s, int32_t n){

    if(n < 1 || n > MAX_JUMP){

        cout << " Error : the jump must be from 1 to " << MAX_JUMP << endl;
        return false;
    }

    return moveBy(s, -n);
}

// =============================================================================================
// PART E: PRINTING
// =============================================================================================

void printVariables(const Variable vars[], int32_t count){

    if(count == 0){

        cout << "(none)";
        return;
    }

    for(int32_t i = 0; i < count; i++){

        if(i > 0){

            cout << ", ";
        }

        cout << vars[i].name << " = " << vars[i].value;
    }
}

void printSnapshot(int32_t step, int32_t totalSteps, const Snapshot &s)
{

    cout << endl;
    cout << "Step " << step << " of " << totalSteps << " (call stack depth " << s.stackDepth << ")" << endl;

    if(s.stackDepth == 0){

        cout << " The program has finished , the call stack is empty" << endl;
        return;

    }

    for(int32_t i = 0; i < s.stackDepth; i++){

        const Frame &frame = s.callStack[i];

        cout << " #" << i << " " << frame.func_name;

        if(i == 0){
            cout << "   <- running here ";
        }

        cout << endl;

        cout << "      args:   ";
        printVariables(frame.argv, frame.argc);
        cout << endl;

        cout << "      locals:   ";
        printVariables(frame.locals, frame.localCount);
        cout << endl;

    }

}

void printHelp()
{
    // Print the commands

    cout << "                                        Commands:                            " << endl;
    cout << " start <file>            send a source filr to the server and open the result " << endl;
    cout << " next                    go one step forward " << endl;
    cout << " prv                     go one step back " << endl;
    cout << " jmp next <n>           jump n steps forwrd " << endl;
    cout << " jmp prv <n>            jump n steps back " << endl;
    cout << " help                    show this list " << endl;
    cout << " quit                    leave the debugger " << endl;
}

// =============================================================================================
// PART F: THE INFINITE LOOP
// =============================================================================================

bool parsePositive(const string &txt, int64_t maxVal, int32_t &out){

    if(txt.empty() || txt.size() > 10){

        return false;
    }

    int64_t val = 0;

    for(size_t i = 0; i < txt.size(); i++){

        if(txt[i] < '0' || txt[i] > '9'){

            return false;
        }

        val = val * 10 + static_cast<int64_t>(txt[i] - '0');
    }

    if(val < 1 || val > maxVal){

        return false;
    }

    out = static_cast<int32_t>(val);
    return true;
}

bool runCommand(DebugSession &s, const string &line, bool &quitRequested)
{
    // 1. Split the line into words.
    // 2. If the first word is "ttdb", drop it.
    // 3. Match the first word:
    //      start      -> needs a path after it, then cmdStart
    //      next       -> no more words, then cmdNext
    //      prv        -> no more words, then cmdPrv
    //      jmp        -> second word "next" or "prv", third word a positive number ,
    //                    then cmdJmpNext or cmdJmpPrv
    //      help       -> printHelp
    //      quit, exit -> set quitRequested = true
    // 4. For anything else, or a wrong number of words, print "unknown command" and the help text, and return false.

    vector<string> words;
    istringstream input(line);
    string word;

    while(input >> word){

        words.push_back(word);
    }

    if(!words.empty() && words[0] == PROGRAM_NAME){

        words.erase(words.begin());
    }

    if(words.empty()){

        return true;
    }

    const string &cmd = words[0];

    if(cmd == "start" && words.size() == 2){

        return cmdStart(s, words[1]);
    }
    else if(cmd == "next" && words.size() == 1){

        return cmdNext(s);
    }
    else if(cmd == "prv" && words.size() == 1){

        return cmdPrv(s);
    }
    else if(cmd == "jmp" && words.size() == 3 && (words[1] == "next" || words[1] == "prv")){

        int32_t n;

        if(!parsePositive(words[2], MAX_JUMP, n)){

            cout << " Jump needs a whole number from 1 to " << MAX_JUMP << endl;
            return false;
        }

        if(words[1] == "next"){

            return cmdJmpNext(s, n);
        }

        return cmdJmpPrv(s, n);
    }
    else if(cmd == "help" && words.size() == 1){

        printHelp();
        return true;
    }
    else if((cmd == "quit" || cmd == "exit") && words.size() == 1){

        quitRequested = true;
        return true;
    }

    cout << "Unknown command. " << endl;
    printHelp();
    return false;

}

int main(int argc, char *argv[])
{
    // you get string LB_HOST and int LB_PORT = 0 in arguments of this main function.

    // DebugSession session; initSession(session);
    // Print a short welcome and the help text.
    //
    // Infinite loop:
    //   if quitRequested, break
    //
    // After the loop: closeSession(session) and return 0.
    // Never exit the loop because of an error. Only quit or the end of the input leaves it.

    if(argc != 1 && argc != 3){

        cout << "Usage : " << argv[0] << "<lb_ip><lb_port>" << endl;
        return 1;
    }

    if(argc == 3){

        int32_t port;

        if(!parsePositive(argv[2], 65535, port)){

            cout << " The port must be  a number from 1 to 65535 " << endl;
            return 1;
        }

        LB_HOST = argv[1];
        LB_PORT = port;
    }

    DebugSession session;
    initSession(session);

    cout << " Time - Travel Debugger" << endl;
    printHelp();

    bool quitRequested = false;
    string line;

    while(true){

        if(quitRequested){
            break;
        }

        cout << PROMPT << flush;

        if(!getline(cin, line)){
            break;
        }

        runCommand(session, line, quitRequested);
    }

    closeSession(session);
    return 0;

}