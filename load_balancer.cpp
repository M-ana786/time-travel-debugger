// ======================= TIME-TRAVEL DEBUGGER - LOAD BALANCER TEMPLATE =======================

// What load balancer does:
//   1. Many clients can connect at the same time. Each one sends one string (the source code). 
//   2. Clients wait in a queue (first come, first served).
//   3. For each client, in order: make sure the server is running (start it with system() if not),
//      pass the string to the server, pass the server's reply back to the client.
//
// It does not read or understand the messages. It only moves bytes.
// The only messages it writes itself are errors ("server busy", "server failed to start").
//
// Two threads:
//   acceptor thread (acceptLoop) -> takes new clients and puts them in the queue. It never waits for the server.
//   main thread (dispatcherLoop) -> takes ONE client out of the queue and serves it completely, then the next.
//
// How the server is started:
//   system("./server 5001 &");
//   The "&" is required. Without it, system() waits until the server exits and the load balancer freezes.
//
//
// What the server program must do (the load balancer depends on this):
//   - it takes its port as the first argument, for example: ./server 5001
//   - it reads the string until the connection's write side is closed (recv returns 0)
//   - it sends its reply as [status: 1 byte][payload], then closes the connection 


#include <iostream>
#include <string>
#include <cstdlib>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "helper.h"
#include <stdexcept>
#include <cstdio>
#include <vector>
#include <cerrno>
#include <ctime>
#include <sys/time.h>
#include <atomic>
#include <poll.h>
using namespace std;


// ---- Constants ----
const char *SERVER_PATH = "./server";      // the program we start when the server is down    
const int LISTEN_BACKLOG = 16;             // connections the OS holds until accept() takes them
const int MAX_PENDING_CLIENTS = 8;         // clients allowed to wait in our queue. More than this get "server busy".
const int STARTUP_MAX_ATTEMPTS = 50;       // after starting the server, how many times we try to connect
const int STARTUP_POLL_INTERVAL_MS = 100;  // wait between those tries (50 x 100 ms = 5 seconds in total)
const int CLIENT_WAIT_LIMIT_SEC = 30;      // a client that waited longer than this in the queue is dropped (client times out at 30s)
const int CLIENT_IO_TIMEOUT_SEC = 10;      // max silence while talking to a client before we give up on it
const int SERVER_IO_TIMEOUT_SEC = 25;      // max silence while waiting for the server (it has to build the whole .tdbg)
const int DISPATCH_IDLE_US = 10 * 1000;    // dispatcher naps this long when the queue is empty
int lbPort = 5000;
string serverHost = "127.0.0.1";
int serverPort = 5001;


// ---- Queue: here it holds client sockets (int). ----
// Implement it yourself, do not use std::queue.
template <typename T>
class Queue
{
    struct Node{

        T data;
        Node *next;
    };
    Node *front, *rear;
    int count;


public:
    // Implement these functions:
    Queue(){

        
       front = rear = nullptr;
       count = 0;

    }
    ~Queue(){

        while(!isEmpty()){
            dequeue();
        }

    }
    void enqueue(const T &val){

        Node* temp = new Node();
        temp->data = val;
        temp->next = nullptr;

        if (isEmpty()){
        
            front = rear = temp;
        }else{
        
            rear->next = temp;
            rear = temp;
     
        }
        count++;    
    }
    T dequeue(){

        if(isEmpty()){
            throw underflow_error("Empty queue");
        }

        
        Node* temp = front;
        T val = temp->data;
        front = front->next;
       
        if(front == nullptr){
            rear = nullptr;
        }

        delete temp;
        count--;
        return val;
    }
    bool isEmpty(){

        return count == 0;
    }
    int size(){

        return count;
    }
};

struct ClientInfo
{
    int sock;           // the connected socket
    string address;     // the client's IP as text, e.g. "127.0.0.1"
    int port;           // the client's port
    time_t arrivedAt;   // when accept() returned
};

// The queue is used by two threads, so every use of it must be inside queueMutex.
Queue<ClientInfo> pendingClients;
mutex queueMutex;
static_assert(atomic<bool>::is_always_lock_free, "Shutdown flag must be lock-free");
atomic<bool> flag{true};


// ---- Basic socket helpers ----


static bool setSocketTimeouts(int sock, int seconds)
{
    timeval tv{};
    tv.tv_sec = seconds;
    tv.tv_usec = 0;
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0){

        perror("setsockopt SO_RCVTIMEO");
        return false;
    }
    if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0){

        perror("setsockopt SO_SNDTIMEO");
        return false;
    }
    return true;

}


static bool parsePort(const char *txt, int &out)
{
    char *end = nullptr;
    errno = 0;
    long v = strtol(txt, &end, 10);

    if (errno != 0 || end == txt || *end != '\0' || v < 1 || v > 65535){
    
        return false;
    }
    
    out = static_cast<int>(v);
    return true;
}

int createListenSocket(int port)
{ 
    // Create a TCP socket with SOCK_CLOEXEC(flag).
    // set SO_REUSEADDR so we can restart the load balancer right away.
    // bind() to the port on any address, then listen(LISTEN_BACKLOG).
    // If bind or listen fails, print the error and exit, because without this socket we cannot work.
    if (port < 1 || port > 65535){

        cerr << "Error: invalid load balancer port" << endl;
        exit(EXIT_FAILURE);
    }

    int sock = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (sock < 0){

        perror("socket");
        exit(1);
    }

    int yes = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0){

        perror("setsockopt SO_REUSEADDR");
        close(sock);
        exit(EXIT_FAILURE);
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (bind(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0){

        perror("bind");
        close(sock);
        exit(1);
    }

    if (listen(sock, LISTEN_BACKLOG) < 0){

        perror("listen");
        close(sock);
        exit(1);
    }
    return sock;


}
int connectToServer(const char *host, int port)
{
    // Create a TCP socket (SOCK_CLOEXEC) and connect it to host:port.
    // Return the socket, or -1 if the connection fails
    if (port < 1 || port > 65535){

        return -1;
    }

    int sock = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (sock < 0){

        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0){

        close(sock);
        return -1;
    }

    if (connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0){

        close(sock);
        return -1;
    }
    return sock;
    
}

// =============================================================================================
// PART 1: WAKING UP THE SERVER IF IT IS DOWN
// =============================================================================================


bool startServer(int port)
{
    // Build the command text: SERVER_PATH + " " + port as text + "&"
    //   Example: "./server 5001 &"
    // Call system() with it.
    // Return true if system() returned 0, otherwise false.
    string cmd = string(SERVER_PATH) + " " + to_string(port) + " &";
    return system(cmd.c_str()) == 0;


}
int connectToServerWithWakeup()
{
    // 1. Try connectToServer(). If it works, return that socket. The server is alive.
    // 2. Otherwise call startServer(). If it fails, return -1.
    // 3. Try connectToServer() up to STARTUP_MAX_ATTEMPTS times 
    // 4. Return the first working socket, or -1 if the server never came up.
   
    int sock = connectToServer(serverHost.c_str(), serverPort);
    if (sock >= 0){

        return sock;
    }

   
    cout << "[LB] server is down, starting " << SERVER_PATH << " " << serverPort << endl;
    if (!startServer(serverPort)){

        return -1;
    }

   
    for (int i = 0; i < STARTUP_MAX_ATTEMPTS; i++){

        usleep(STARTUP_POLL_INTERVAL_MS * 1000);
        sock = connectToServer(serverHost.c_str(), serverPort);
        
        if (sock >= 0){

            return sock;
        }
    }
   
    return -1;

}


// =============================================================================================
// PART 2: PASSING THE STRING AND THE REPLY (one client, called by the main thread only)
// =============================================================================================

 static void relay(int clientSock, int serverSock){
    vector<char> buf(IO_BUFFER_SIZE);
    vector<char> body(IO_BUFFER_SIZE); 

    
    char status;
    if (!recvAll(clientSock, &status, 1)) {

        sendError(clientSock, "upload failed: nothing received");
        return;
    }

    TrailerChecker upCheck(status);
    Crc32 upCrc; 
    upCrc.update(&status, 1);

    if (!sendAll(serverSock, &status, 1)){

        sendError(clientSock, "backend server connection lost");
        return;
    }

    while (true){
        
        ssize_t r = recv(clientSock, buf.data(), buf.size(), 0);
        if (r < 0) {
            
            if (errno == EINTR){
                continue;
            }

            sendError(clientSock, "upload failed or timed out");
            return;
        }
        if (r == 0){
            break; 
        }

        size_t released = upCheck.feed(buf.data(), static_cast<size_t>(r), body.data());
        if (released > 0){
            
            upCrc.update(body.data(), released);
            if (!sendAll(serverSock, body.data(), released)){
               
                sendError(clientSock, "backend server connection lost");
                return;
            }
        }
    }

    if (!upCheck.verify()){
       
        shutdown(serverSock, SHUT_WR);
        sendError(clientSock, "upload checksum mismatch");
        return;
    }

    char tail[4];
    write_u32(tail, upCrc.value()); 
    
    if (!sendAll(serverSock, tail, 4)){
        
        sendError(clientSock, "backend server connection lost");
        return;
    }

    shutdown(serverSock, SHUT_WR); 

  
    char rstatus;
    if (!recvAll(serverSock, &rstatus, 1)) {

        sendError(clientSock, "no reply from backend server");
        return;
    }

    TrailerChecker downCheck(rstatus);
    Crc32 downCrc;
    downCrc.update(&rstatus, 1);

    if (!sendAll(clientSock, &rstatus, 1)){
        
        return; 
    }

    while (true){
       
        ssize_t r = recv(serverSock, buf.data(), buf.size(), 0);
        if (r < 0){
            
            if (errno == EINTR){
                
                continue;
            }
            cout << "[LB] lost the server mid-reply, trailer withheld" << endl;
            return; 
        }
        if (r == 0){
            
            break; 
        }

        size_t released = downCheck.feed(buf.data(), static_cast<size_t>(r), body.data());
        if (released > 0){

            downCrc.update(body.data(), released);
            if (!sendAll(clientSock, body.data(), released)){
               
                return; 
            }
        }
    }

    if (!downCheck.verify()){
       
        cout << "[LB] reply checksum mismatch, trailer withheld" << endl;
        return; 
    }

    write_u32(tail, downCrc.value());
    sendAll(clientSock, tail, 4);
}

void relayOneClient(ClientInfo &c)
{
    // 1. serverSock = connectToServerWithWakeup().
    //    If it is -1: sendError "backend server failed to start", close clientSock, return.
    // 3. Client -> server: repeat recv() from clientSock into a buffer of IO_BUFFER_SIZE and sendAll() it to serverSock.
    //    Stop when recv() returns 0 (the client finished sending) or fails.
    // 4. Call shutdown(serverSock, SHUT_WR), so the server knows the string is complete.
    // 5. Server -> client: repeat recv() from serverSock and sendAll() to clientSock.
    //    Stop when recv() returns 0 (the server sent its whole reply and closed) or fails.
    // 6. Close both sockets.
    if (!setSocketTimeouts(c.sock, CLIENT_IO_TIMEOUT_SEC)){
        
        return;
    }

    int serverSock = connectToServerWithWakeup();
    if (serverSock < 0){

        sendError(c.sock, "backend server failed to start");
        return;
    }

    if (!setSocketTimeouts(serverSock, SERVER_IO_TIMEOUT_SEC)){
       
        close(serverSock);
        sendError(c.sock, "backend connection setup failed");
        return;
    }
    
    relay(c.sock, serverSock);
    close(serverSock); 
}    

// =============================================================================================
// PART 3: THE TWO THREADS
// =============================================================================================


void acceptLoop(int listenSock)
{
    // Repeat: accept() a new client. 
    //  If pendingClients.size() is already MAX_PENDING_CLIENTS: unlock, sendError "server busy, try again later",
    // else
    // Lock queueMutex.
    //   close the client, and go on.
    //   Otherwise enqueue the client socket.
    // Never wait for the server here, so new clients are always accepted quickly.
    // If accept() fails, print the error and keep going (do not end the thread).
    while (flag.load()){
        
        pollfd watched{};
        watched.fd = listenSock;
        watched.events = POLLIN;

        int ready = poll(&watched, 1, 100);
        if (ready < 0){

            if (errno == EINTR) {
                
                continue;
            }
            perror("poll listener");
            flag.store(false);
            break;
        }
       
        if (ready == 0 || !(watched.revents & POLLIN)){

             if (watched.revents & (POLLERR | POLLHUP | POLLNVAL)){

                cerr << "Listening socket stopped working" << endl;
                flag.store(false);
                break;
            }
            continue;
        }
       
        if (!flag.load()){

            break;
        }

        sockaddr_in caddr{};
        socklen_t clen = sizeof(caddr);
        int cs = accept4(listenSock, reinterpret_cast<sockaddr *>(&caddr),
                         &clen, SOCK_CLOEXEC);
        if (cs < 0){

            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK){

                continue;
            }

            perror("accept4");
            continue;
        }
        
        ClientInfo c{};
        c.sock = cs;
        char ip[INET_ADDRSTRLEN] = "?";
        
        if (inet_ntop(AF_INET, &caddr.sin_addr, ip, sizeof(ip)) != nullptr){

            c.address = ip;
        }else{

            c.address = "unknown";
        }

        c.port = ntohs(caddr.sin_port);
        c.arrivedAt = time(nullptr);

        bool queued = false;
        {

            lock_guard<mutex> lock(queueMutex);
            if (flag.load() && pendingClients.size() < MAX_PENDING_CLIENTS){

                pendingClients.enqueue(c);
                queued = true;
            }
        }
        if (!queued){

            if (flag.load()){

                cout << "[LB] queue full, refusing " << c.address << ":" << c.port << endl;
                setSocketTimeouts(cs, 2);
                sendError(cs, "server busy, try again later");
                shutdown(cs, SHUT_WR);
                
                char junk[1024];
                for (int i = 0; i < 16; i++){

                    ssize_t n = recv(cs, junk, sizeof(junk), MSG_DONTWAIT);
                    
                    if (n > 0){

                        continue;
                    }

                    if (n < 0 && errno == EINTR){
                        
                        i--;
                        continue;
                    }
                    break;
                }
            }
            close(cs);
        }
    }



}
void dispatcherLoop()
{
    // This runs in the main thread and never ends.
    // Repeat:
    //   Lock queueMutex, take ONE client out of the queue if there is one, unlock.
    //   If we got a client: call relayOneClient(). The next client is taken out of the queue only after this one is finished.
     while (flag.load()){

        ClientInfo c{};
        bool got = false;
        {

            lock_guard<mutex> lock(queueMutex);
            if (!pendingClients.isEmpty()){

                c = pendingClients.dequeue();
                got = true;
            }
        }

        if (!got){

            usleep(DISPATCH_IDLE_US);
            continue;
        }

        if (difftime(time(nullptr), c.arrivedAt) > CLIENT_WAIT_LIMIT_SEC){

            cout << "[LB] " << c.address << ":" << c.port << " waited too long, dropped" << endl;
            close(c.sock);
            continue;
        }

        cout << "[LB] serving " << c.address << ":" << c.port << endl;
        relayOneClient(c);
        close(c.sock);
        cout << "[LB] done with " << c.address << ":" << c.port << endl;
    }
}


void handler(int sg) {
    (void)sg;
    flag = 0;
}
int main(int argc, char *argv[])
{

    if (argc != 1 && argc != 3){

        cerr << "Usage: " << argv[0] << " <server_ip> <server_port>" << endl;
        return 1;
    }

    if (argc == 3){

        in_addr testAddress{};
        if (inet_pton(AF_INET, argv[1], &testAddress) != 1){

            cerr << "Error: server IP must be a valid IPv4 address" << endl;
            return 1;
        }

        serverHost = argv[1];
        if (!parsePort(argv[2], serverPort)){

            cerr << "Error: server port must be a number from 1 to 65535" << endl;
            return 1;
        }
    }

    struct sigaction sa{};
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGINT, &sa, nullptr) < 0 || sigaction(SIGTERM, &sa, nullptr) < 0){

        perror("sigaction");
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);

    int listenSock = createListenSocket(lbPort);
    cout << "[LB] listening on port " << lbPort << ", server at " << serverHost << ":" << serverPort << endl;

    thread acceptor;
    try{
        acceptor = thread(acceptLoop, listenSock);
    }
    catch (const exception &e){

        cerr << "Could not start acceptor thread: " << e.what() << endl;
        close(listenSock);
        return 1;
    }

    dispatcherLoop();

   
    if (acceptor.joinable()){

        acceptor.join();
    }

    close(listenSock);
    {

        lock_guard<mutex> lock(queueMutex);
        while (!pendingClients.isEmpty())
        {
            close(pendingClients.dequeue().sock);
        }
    }

    cout << "[LB] stopped" << endl;
    return 0;
}
