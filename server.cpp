// ======================= TIME-TRAVEL DEBUGGER - SERVER TEMPLATE =======================

// Pipeline this file implements, top to bottom:
//   0. Receive  -- stream the client's .trace bytes straight to source.bin on disk
//   1. Pass 0X0   -- validity check (FUNC/FUNC_END matching)
//   2. Pass 0X1   -- resolve(): copy EVERY source line into resolve.bin as [offset][size][string], then patch CALL targets.
//   3. Pass 0X2   -- execute resolve.bin: tokenize ONE line at a time, update the call stack, take a snapshot -> Timeline
//   4. Pass 0X3   -- serialize Timeline -> session.tdbg(header + snapshot records + dense index)

#include <iostream>
#include <string>
#include <cstdint>
#include <fstream>
#include <unistd.h>
#include <sys/socket.h>
#include <cstdio>
#include <stdexcept>
#include "helper.h"
#include <csignal>
#include <cerrno>
#include <cstdlib>
#include <vector>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
using namespace std;

// ---- Constants ----

const int32_t MAX_FUNCS = 128;
const int32_t MAX_TOKENS = MAX_VARS_PER_FRAME + 2; // kW + func_name + upto 16 params/args
const int32_t MAX_PATCHES = MAX_FUNCS * 4;
const uint64_t MAX_SOURCE_BYTES = 15ULL * 1024 * 1024; // sanity cap on the declared file length
const int32_t SOCKET_TIMEOUT_SEC = 5;                      // TODO: apply as SO_RCVTIMEO so a deadclient can't hang the server forever
const int SERVER_BACKLOG = 16;
const string SERVER_SOURCE_FILE = "ttdb_server_source_" + to_string(getpid()) + ".bin";
const string SERVER_RESOLVE_FILE = "ttdb_server_resolve_" + to_string(getpid()) + ".bin";
const string SERVER_TDBG_FILE = "ttdb_server_session_" + to_string(getpid()) + ".tdbg";
const int DEFAULT_PORT = 5001;

// ---- Custom data structures

// Stack: back the live Call Stack during execution
template <typename T>
class Stack
{
    struct Node
    {
        T data;
        Node *next;
    };

    Node *top;
    int32_t count;

public:
    // Implement these functions:
    Stack()
    {
        top = nullptr;
        count = 0;
    }

    ~Stack(){
      
        while(top != nullptr){
      
            Node* temp = top;
            top = top->next;
            delete temp;
        }
    }

    void push(const T &val)
    {
        if(count == MAX_STACK_DEPTH){
      
            throw overflow_error("Stack is full");
        }

        Node* insertion_node = new Node();
        insertion_node->data = val;
        insertion_node->next = top;
        top = insertion_node;
        count++;
    
    }

    T pop()
    {
        if(count == 0 || top == nullptr){
      
            throw underflow_error("Empty stack");
        }

        Node* to_be_deleted = top;
        T val = top->data;
        top = top->next;
        delete to_be_deleted;
        count--;
        return val;
    }

    T &peek()
    {
        if(count == 0 || top == nullptr){
    
            throw underflow_error("Empty stack");
        }

        return top->data;
    }

    bool isEmpty()
    {
    
        return count == 0;
    }

    int32_t depth()
    {
    
        return count;
    }

    int32_t snapshot_into(T out[], int32_t maxLen)
    {
        // copies every frame, top to bottom in the array given as a parameter
        // this is what buildSnapshot() call, returns count written

        int32_t indx = 0;
        Node* c_n = top; // current node

        while(c_n != nullptr && indx < maxLen){
    
            out[indx] = c_n->data;
            c_n = c_n->next;
            indx++;
        }

        return indx;
    }


};

// Timeline : doubly linked list of Snapshots
struct Snapshot; // fwd declaration;

struct TimelineNode
{
    Snapshot *data;
    TimelineNode *next;
    TimelineNode *prev;
};

class Timeline
{
    TimelineNode *head, *tail;
    int32_t stepCount;

public:
    // Implement these functions
    Timeline()
    {
        head = tail = nullptr;
        stepCount = 0;
    }

    ~Timeline();

    void record(Snapshot *s)
    {
        // add record in the timeline

        TimelineNode* new_node = new TimelineNode();
        new_node->data = s;
        new_node->next = nullptr;
        new_node->prev = tail;

        if(head == nullptr){
    
            head = tail = new_node;
        }else{
    
            tail->next = new_node;
            tail = new_node;
        }

    
        stepCount++;
    }

    TimelineNode *begin()
    {
    
        if(stepCount == 0 || head == nullptr){
    
            throw underflow_error("Empty Timeline");
        }

        return head;
    }

    int32_t getStepCount()
    {
        return stepCount;
    }
};

// Core structs

Timeline::~Timeline(){
    
    TimelineNode* c_n = head;

    while(c_n != nullptr){
    
        TimelineNode* n = c_n->next;
        delete c_n->data;
        delete c_n;
        c_n = n;
    }
}

void writeHeader(FILE* f , const TTDBHeader& h){
    
    if(fwrite(h.magic , 1, 4, f) != 4){
    
        throw runtime_error("Failed to write TTDB magic ");
    }

    if(fwrite(&h.version , sizeof(int32_t), 1, f) != 1){

        throw runtime_error("Failed to write TTDB version ");
    }

    if(fwrite(&h.stepCount , sizeof(int32_t), 1, f) != 1){

        throw runtime_error("Failed to write TTDB step count ");
    }

    if(fwrite(&h.indexOffset , sizeof(int64_t), 1, f) != 1){

        throw runtime_error("Failed to write TTDB index offset ");
    }
}

// PASS 0x0: READING source.bin + VALIDITY CHECK

bool is_space(char ch){

    if(ch == ' ' || ch == '\t' || ch == '\r'){

        return true;
    }

    return false;
}

bool readSourceLine(ifstream &in, string &out)
{
    // reads the next nonblank line

    string line;

    while(getline(in , line)){

        int line_length = line.length();
        int indx = 0;

        while(indx < line_length && is_space(line[indx])){

            indx++;
        }

        if(indx < line_length){

            out = line;
            return true;

        }
    }

    return false;

}

string firstWord(const string &line)
{
    string first_word;
    int indx = 0;
    int len = line.length();

    while(indx < len && is_space(line[indx])){

        indx++;
    }

    while(indx < len && !is_space(line[indx])){

        first_word = first_word + line[indx];
        indx++;
    }

    return first_word;
}

string secondWord(const string &line)
{
    string second_word;
    int indx = 0;
    int len = line.length();

    while(indx < len && is_space(line[indx])){

        indx++;
    }

    while(indx < len && !is_space(line[indx])){

        indx++;
    }

    while(indx < len && is_space(line[indx])){

        indx++;
    }

    while(indx < len && !is_space(line[indx])){

        second_word = second_word + line[indx];
        indx++;
    }

    return second_word;
}


bool is_num(const string& str);

bool validateProgram(const char *sourcePath, string &err)
{
    ifstream fin(sourcePath);

    if(!fin){

        err = "server couldn't open source.bin";
        return false;
    }

    string Line;
    bool inside = false;

    string functionNames[MAX_FUNCS];
    int32_t functionCount = 0;

    while(readSourceLine(fin, Line)){

        if(Line.size() > static_cast<size_t>(MAX_NAME_LENGTH)){

            err = "line too long (max " + to_string(MAX_NAME_LENGTH) + " characters)";
            return false;
        }

        string word = firstWord(Line);
        string name = secondWord(Line);

        // Count the total number of words
        int32_t wordCount = 0;
        bool inWord = false;

        for(int32_t i = 0; i < static_cast<int32_t>(Line.size()); i++){

            if(!is_space(Line[i])){
            
                if(!inWord)
                {
                    wordCount++;
                    inWord = true;
                }
            }else{
            
                inWord = false;
            }
        }

        if(word == "func"){
            
            if(inside){
            
                err = "nested function declaration: " + Line;
                return false;
            }

            if(name == "" || is_num(name)){
            
                err = "invalid function name: " + Line;
                return false;
            }

            if(wordCount - 2 > MAX_VARS_PER_FRAME){
            
                err = "too many function parameters: " + Line;
                return false;
            }

            if(functionCount >= MAX_FUNCS){
            
                err = "too many functions";
                return false;
            }

            for(int32_t i = 0; i < functionCount; i++){
            
                if(functionNames[i] == name){
            
                    err = "duplicate function definition: " + name;
                    return false;
                }
            }

            functionNames[functionCount++] = name;


            string params[MAX_VARS_PER_FRAME];
            int32_t paramCount = 0;
            int32_t wordIndex = 0;
            int32_t indx = 0;

            while(indx < static_cast<int32_t>(Line.size())){
            
                while(indx < static_cast<int32_t>(Line.size()) && is_space(Line[indx])){
            
                    indx++;
                }

                if(indx >= static_cast<int32_t>(Line.size())){
            
                    break;
                }

                string param;

                while(indx < static_cast<int32_t>(Line.size()) && !is_space(Line[indx])){
            
                    param += Line[indx++];
                }

                wordIndex++;

                if(wordIndex <= 2){
            
                    continue;
                }

                if(is_num(param)){
            
                    err = "numeric parameter name is invalid: " + Line;
                    return false;
                }

                for(int32_t i = 0; i < paramCount; i++){
            
                    if(params[i] == param){
            
                        err = "duplicate parameter name: " + param;
                        return false;
                    }
                }

                params[paramCount++] = param;
            }

            inside = true;
        }
        else if(word == "func_end"){
            
            if(wordCount != 1){
            
                err = "func_end must not have operands: " + Line;
                return false;
            }

            if(!inside){
            
                err = "func_end without a matching func";
                return false;
            }

            inside = false;
        }else{
            
            if(!inside){
            
                err = "instruction outside a function: " + Line;
                return false;
            }

            if(word == "call"){
            
                if(wordCount < 2 || wordCount > MAX_VARS_PER_FRAME + 2){
            
                    err = "invalid call instruction: " + Line;
                    return false;
                }

                if(is_num(name)){
            
                    err = "invalid function name in call: " + Line;
                    return false;
                }
            }
            else if(word == "set"){
            
                if(wordCount != 3){
            
                    err = "set requires exactly 2 operands: " + Line;
                    return false;
                }

                if(is_num(name)){
            
                    err = "invalid variable name: " + Line;
                    return false;
                }
            }
            else if(word == "add" || word == "sub" || word == "mul" || word == "div"){
            
                if(wordCount != 3){
            
                    err = word + " requires exactly 2 operands: " + Line;
                    return false;
                }

                if(is_num(name)){
            
                    err = "invalid variable name: " + Line;
                    return false;
                }
            }else{
            
                err = "unknown instruction: " + Line;
                return false;
            }
        }
    }

    if(fin.bad()){
        
        err = "error while reading source.bin";
        return false;
    }

    if(inside){
        
        err = "func without a matching func_end";
        return false;
    }

    return true;
}

// PASS 0x1: RESOLVE() -> resolve.bin

int64_t writeResolveRecord(FILE *f, int64_t offsetField, const string &text)
{
    // writes one [offset(8B)][size(4B)][string] record at the current file position
    // returns this record's own starting byte position
    int64_t record_start = ftell(f); // this gives current position of file

    if(record_start == -1){
   
        cout << "Error getting file position" << endl;
        return -1;
    }

    if(fwrite(&offsetField, sizeof(int64_t), 1, f) != 1){
   
        cout << "Error writing offset field" << endl;
        return -1;
    }  

    int32_t string_size = static_cast<int32_t>(text.size());

    
    if(fwrite(&string_size , sizeof(int32_t), 1, f) != 1){
    
        cout << "Error writing string size" << endl;
        return -1;
    }

    if(fwrite(text.data(), 1, text.size(), f) != text.size()){
    
        cout << "Error writing string" << endl;
        return -1;
    }

    return record_start;
}

int64_t readResolveRecord(FILE *f, string &outText)
{
    int64_t offset_field;

    
    if(fread(&offset_field, sizeof(int64_t), 1, f) != 1){
    
        return -1;
    }

    int32_t string_size;

    if(fread(&string_size, sizeof(int32_t), 1, f) != 1){
    
        return -1;
    }

    if(string_size < 0 || static_cast<uint64_t>(string_size) > MAX_SOURCE_BYTES){
    
        return -1;
    }

    outText.resize(string_size);

    if(string_size > 0){
    
        if(fread(&outText[0], 1, string_size, f) != static_cast<size_t>(string_size)){
    
            return -1;
        }
    }

    return offset_field;
}

bool is_num(const string& str);

int64_t resolveProgram(const char *sourcePath, const char *resolveBinPath , string &err)
{
    
    FuncEntry funcArray[MAX_FUNCS];
    int32_t funcCount = 0;
    PendingPatch patches[MAX_PATCHES];
    int32_t patchCount = 0;

    // Every source line becomes one record holding the raw line, as-is.
    // resolve() only PEEKS at the leading word(s) -- enough to spot FUNC
    // (remember its position) and CALL (remember which function it needs
    // and where its offset field sits).
    // Once the whole file is written, every CALL's offset field is patched
    // with its target's position. Patching happens after the full write
    // Returns the byte offset of main's FUNC header record.
    // if there is no main return the error

    ifstream fin(sourcePath);
    FILE* f = fopen(resolveBinPath, "wb");

    if(!fin || f == nullptr){
    
        if(f != nullptr){
    
            fclose(f);
        }

        err = "server couldn't open source.bin / resolve.bin";
        return -1;
    }

    string Line;

    while(readSourceLine(fin , Line)){
    
        int64_t offset_field = ftell(f);

        if(offset_field == -1){
    
            err = "internal error: ftell failed on resolve.bin";
            fclose(f);
            return -1;
        }

        int64_t r_st_p = writeResolveRecord(f, offset_field, Line); // r_st_p is records starting point

        if(r_st_p == -1){
    
            err = "internal error: couldn't write resolve.bin";
            fclose(f);
            return -1;
        }

        string word = firstWord(Line);

        if(word == "func"){
    
            if(funcCount >= MAX_FUNCS){
    
                err = "too many functions (max " + to_string(MAX_FUNCS) + ")";
                fclose(f);
                return -1;
            }

            string func_name = secondWord(Line);

            if(func_name == ""){
    
                err = "func declaration without a name";
                fclose(f);
                return -1;
            }

            funcArray[funcCount].funcName = func_name;
            funcArray[funcCount].byteOffsetInResolveBin = r_st_p;
            funcCount++;
    
        }else if(word == "call"){
    
            if(patchCount >= MAX_PATCHES){
    
                err = "too many call instructions (max " + to_string(MAX_PATCHES) + ")";
                fclose(f);
                return -1;
            }

            string func_name = secondWord(Line);

            if(func_name == ""){
    
                for(int32_t j = 0; j < funcCount; j++){
    
                    if(funcArray[j].funcName == func_name){
    
                        err = "duplicate function definition: " + func_name;
                        fclose(f);
                        return -1;
                    }
                }

                err = "call instruction without a function name";
                fclose(f);
                return -1;
            }

            patches[patchCount].targetFuncName = func_name;
            patches[patchCount].byteOffsetOfOffsetField = r_st_p;
            patchCount++;
        }
    }

    if(fin.bad()){
    
        err = "error while reading source.bin";
        fclose(f);
        return -1;
    }

    for(int32_t i = 0; i < patchCount; i++){
    
        string tar_func = patches[i].targetFuncName;
        bool found = false;

        for(int32_t j = 0; j < funcCount; j++){
    
            if(tar_func == funcArray[j].funcName){
    
                int64_t offset_field = patches[i].byteOffsetOfOffsetField;

                if(fseek(f, offset_field, SEEK_SET) != 0){
    
                    err = "internal error: seeking to a call offset field";
                    fclose(f);
                    fin.close();
                    return -1;
                }

                int64_t target_offset = funcArray[j].byteOffsetInResolveBin;

                if(fwrite(&target_offset, sizeof(int64_t), 1, f) != 1){
    
                    err = "internal error: writing a patched call offset";
                    fclose(f);
                    fin.close();
                    return -1;
                }

                found = true;
                break;
            }
        }

        if(!found){
    
            err = "call to undefined function '" + tar_func + "'";
            fclose(f);
            fin.close();
            return -1;
        }
    }

    if(fclose(f) != 0){
    
        err = "internal error: couldn't finish writing resolve.bin";
        return -1;
    }

    for(int32_t j = 0; j < funcCount; j++){
    
        if(funcArray[j].funcName == "main"){
    
            return funcArray[j].byteOffsetInResolveBin;
        }
    }

    err = "no main function found";
    fin.close();
    return -1;
}

// PASS 0x2: EXECUTION (tokenization happens here)

enum TokenType
{
    KEYWORD,
    IDENTIFIER,
    PARAM
};

struct Token
{
    TokenType type;
    string text;
};

int32_t tokenizeLine(const string &line, Token tokens[], int32_t maxTokens)
{
    if(maxTokens <= 0){
    
        return 0;
    }

    int32_t size = static_cast<int32_t>(line.length());
    int32_t indx = 0;
    int32_t ct = 0;

    while(indx < size && is_space(line[indx])){
    
        indx++;
    }

    if(indx >= size){
   
        return 0;
    }

    string first_word;

    while(indx < size && !is_space(line[indx])){
    
        first_word += line[indx++];
    }

    tokens[ct].text = first_word;
    tokens[ct].type = KEYWORD;
    ct++;

    while(indx < size && is_space(line[indx])){
   
        indx++;
    }

    if(indx >= size){
   
        return ct;
    }

    if(ct >= maxTokens){
   
        return ct;
    }

    string second_word;

    while(indx < size && !is_space(line[indx])){
   
        second_word += line[indx++];
    }

    tokens[ct].text = second_word;
    tokens[ct].type = IDENTIFIER;
    ct++;

    while(indx < size){
   
        while(indx < size && is_space(line[indx])){
   
            indx++;
        }

        if(indx >= size){
   
            break;
        }

        if(ct >= maxTokens){
   
            return maxTokens + 1;
        }

        string params;

        while(indx < size && !is_space(line[indx])){
   
            params += line[indx++];
        }

        tokens[ct].text = params;
        tokens[ct].type = PARAM;
        ct++;
    }

    return ct;
}

Snapshot *buildSnapshot(Stack<Frame> &callStack)
{
   
    Snapshot* snapshot = new Snapshot();
    snapshot->stackDepth = callStack.snapshot_into(snapshot->callStack, MAX_STACK_DEPTH);
   
    return snapshot;

}

// helper functions

Variable* find_var(Frame& frame , const string& var_name){

    int count = frame.argc;

    for(int i = 0; i < count; i++){

        if(frame.argv[i].name == var_name){

            return &frame.argv[i];
        }
    }

    for(int i = 0; i < frame.localCount; i++){

        if(frame.locals[i].name == var_name){

            return &frame.locals[i];
        }
    }

    return nullptr;
}

bool is_num(const string& str){

    if(str == ""){

        return false;
    }

    int size = str.length();
    int indx = 0;

    if(str[0] == '-'){

        if(size == 1){

            return false;
        }
        indx = 1;
    }

    while(indx < size){

        if(str[indx] < '0' || str[indx] > '9'){

            return false;
        }
        indx++;
    }

    return true;
}

bool str_to_num(const string& str, int32_t& result)
{
    if(!is_num(str)){

        return false;
    }

    int32_t indx = 0;
    bool neg = false;

    if(str[0] == '-'){

        neg = true;
        indx = 1;
    }

    int64_t num = 0;

    while(indx < static_cast<int32_t>(str.length())){

        int32_t dig = str[indx] - '0';
        num = num * 10 + dig;

        if(neg){

            if(num > 2147483648LL){

                return false;
            }
        }else{

            if(num > 2147483647LL){
            
                return false;
            }
        }

        indx++;
    }

    if(neg){
        
        num = -num;
    }

    result = static_cast<int32_t>(num);
    return true;
}

bool getting_value(Frame& frame, string& str, int32_t& res)
{
    
    if(is_num(str)){
    
        return str_to_num(str, res);
    }

    Variable* var = find_var(frame, str);

    if(var == nullptr){
    
        return false;
    }

    res = var->value;
    return true;
}

bool safe_arithmetic(const string& operation, int32_t left, int32_t right, int32_t& result){
    int64_t value = 0;

    if(operation == "add"){
    
        value = static_cast<int64_t>(left) + right;
    }
    else if(operation == "sub"){
    
        value = static_cast<int64_t>(left) - right;
    }
    else if(operation == "mul"){
    
        value = static_cast<int64_t>(left) * right;
    }
    
    else if(operation == "div"){
    
        if(right == 0){
    
            return false;
        }

        if(left == INT32_MIN && right == -1){
    
            return false;
        }

        value = left / right;
    }
    else{
    
        return false;
    }

    if(value < INT32_MIN || value > INT32_MAX){
    
        return false;
    }

    result = static_cast<int32_t>(value);
    return true;
}

bool executeProgram(const char *resolveBinPath, int64_t mainOffset, Timeline &timeline , string &err)
{
    // initialize the call stack
    // make the main frame
    // push main frame on the call stack

    // implementation:
    // execute line by line, and according to the keyword perform action

    FILE* f = fopen(resolveBinPath, "rb");

    if(f == nullptr){
    
        err = "internal error: couldn't open resolve.bin";
        return false;
    }

    try{
        bool ok = true;
        Stack<Frame> call_stack;
        Token Tokens[MAX_TOKENS];
        string Line;
       
        if(fseek(f, mainOffset, SEEK_SET) != 0){
            err = "internal error: couldn't seek to main";
            fclose(f);
            return false;
        }
        
        int64_t main_offset = readResolveRecord(f, Line);
        
        if(main_offset == -1){
            err = "internal error: couldn't read main";
            fclose(f);
            return false;
        }
            
        int token_ct = tokenizeLine(Line, Tokens, MAX_TOKENS);
        
        if(token_ct < 2 || Tokens[0].text != "func"){
            
            err = "invalid main function record";
            fclose(f);
            return false;
        }
        
        Frame main_frame{};
        main_frame.func_name = Tokens[1].text;
        main_frame.argc = token_ct - 2;
        main_frame.localCount = 0;
        main_frame.returnLine = -1;
        
        for(int i = 0; i < main_frame.argc; i++){
           
            main_frame.argv[i].name = Tokens[i + 2].text;
            main_frame.argv[i].value = 0;
        }
        
        call_stack.push(main_frame);
        timeline.record(buildSnapshot(call_stack));
        
        while(!call_stack.isEmpty()){
            
            if(timeline.getStepCount() >= MAX_STEPS){
               
                err = "program is too long: more than " + to_string(MAX_STEPS) + " steps";
                ok = false;
                break;
            }
            
            int64_t re_ps = ftell(f); // record position
            
            if(re_ps == -1){
                err = "internal error: couldn't determine the file position";
                ok = false;
                break;
            
            }
            
            int64_t Offset_Field = readResolveRecord(f, Line);
            
            if(Offset_Field == -1){
                err = "unexpected end of program (missing func_end?)";
                ok = false;
                break;
            }
            
            token_ct = tokenizeLine(Line, Tokens, MAX_TOKENS);
            
            if(token_ct <= 0){
                err = "invalid instruction: " + Line;
                ok = false;
                break;
            }

            string Keyword = Tokens[0].text;
            Frame& c_at = call_stack.peek();
            
            if(Keyword == "set"){
                
                int32_t val = 0;
                
                if(token_ct != 3 || !getting_value(c_at, Tokens[2].text, val)){
                    
                    err = "bad 'set' instruction (missing or undefined value): " + Line;
                    ok = false;
                    break;
                }

                Variable* var = find_var(c_at, Tokens[1].text);

                if(var == nullptr){
    
                    if(c_at.localCount >= MAX_VARS_PER_FRAME){
    
                        err = "too many variables in function " + c_at.func_name;
                        ok = false;
                        break;
                    }

                    var = &c_at.locals[c_at.localCount];
                    var->name = Tokens[1].text;
                    c_at.localCount++;
                }

                var->value = val;
            }
            else if(Keyword == "div" || Keyword == "mul" || Keyword == "add" || Keyword == "sub"){
    
                if(token_ct != 3){
    
                    err = Keyword + " requires exactly 2 operands: " + Line;
                    ok = false;
                    break;
                }

                Variable* res = find_var(c_at, Tokens[1].text);
                int32_t op = 0;

                if(res == nullptr || !getting_value(c_at, Tokens[2].text, op)){
    
                    err = "undefined variable in: " + Line;
                    ok = false;
                    break;
                }

                int32_t newValue = 0;

                if(Keyword == "div" && op == 0){
    
                    err = "division by zero in: " + Line;
                    ok = false;
                    break;
                }

                if(!safe_arithmetic(Keyword, res->value, op, newValue)){
    
                    err = "arithmetic overflow or invalid operation in: " + Line;
                    ok = false;
                    break;
                }

                res->value = newValue;
            }
            else if(Keyword == "call"){
    
                if(token_ct < 2 || token_ct > MAX_TOKENS){
    
                    err = "invalid call instruction: " + Line;
                    ok = false;
                    break;
                }

                int32_t arg_ct = token_ct - 2;
                string calledFuncName = Tokens[1].text;
                int32_t arg_val[MAX_VARS_PER_FRAME];

                if(arg_ct > MAX_VARS_PER_FRAME){
    
                    err = "too many arguments in: " + Line;
                    ok = false;
                    break;
                }

                bool g_val = true; // got value
                
                for(int i = 0; i < arg_ct; i++){
    
                    if(!getting_value(c_at, Tokens[i + 2].text, arg_val[i])){
    
                        err = "'" + Tokens[i + 2].text + "' is not defined in: " + Line;
                        g_val = false;
                        break;
                    }
                }

                if(g_val == false){
    
                    ok = false;
                    break;
                }

                if(fseek(f, Offset_Field, SEEK_SET) != 0){
    
                    err = "internal error: couldn't seek to the called function";
                    ok = false;
                    break;
                }

                if(readResolveRecord(f, Line) == -1){
    
                    err = "internal error: couldn't read the called function";
                    ok = false;
                    break;
                }

                int hdr_ct = tokenizeLine(Line, Tokens, MAX_TOKENS); // header count

                if(hdr_ct < 2 || Tokens[0].text != "func" || Tokens[1].text != calledFuncName){
    
                    err = "invalid called function header";
                    ok = false;
                    break;
                }

                int par_ct = hdr_ct - 2; // parameter counter

                if(par_ct != arg_ct){
    
                    err = "function " + Tokens[1].text + " needs " + to_string(par_ct) + " arguments but got " + to_string(arg_ct);
                    ok = false;
                    break;
                }

                Frame frame{};
                frame.func_name = Tokens[1].text;
                frame.argc = par_ct;
                frame.localCount = 0;
                frame.returnLine = re_ps;

                for(int i = 0; i < par_ct; i++){
    
                    frame.argv[i].name = Tokens[i + 2].text;
                    frame.argv[i].value = arg_val[i];
                }

                if(call_stack.depth() >= MAX_STACK_DEPTH){
    
                    err = "stack overflow: calls are nested more than " + to_string(MAX_STACK_DEPTH) + " deep";
                    ok = false;
                    break;
                }

                
                call_stack.push(frame);
            }
            else if(Keyword == "func_end"){
    
                Frame completed = call_stack.pop();

                if(completed.returnLine != -1){
    
                    if(fseek(f, completed.returnLine, SEEK_SET) != 0){
    
                        err = "internal error: couldn't return to the caller";
                        ok = false;
                        break;
                    }

                    if(readResolveRecord(f, Line) == -1){
    
                        err = "internal error: couldn't read the caller instruction";
                        ok = false;
                        break;
                    }

                    int caller_token_ct = tokenizeLine(Line, Tokens, MAX_TOKENS);

                    if(caller_token_ct < completed.argc + 2){
    
                        err = "invalid caller instruction";
                        ok = false;
                        break;
                    }

                    if(call_stack.isEmpty()){
    
                        err = "internal error: no caller frame exists";
                        ok = false;
                        break;
                    }

                    Frame& caller_frame = call_stack.peek();

                    for(int i = 0; i < completed.argc; i++){
    
                        Variable* var = find_var(caller_frame, Tokens[i + 2].text);

                        if(var != nullptr){
    
                            var->value = completed.argv[i].value;
                        }
                    }
                }
            }else{
    
                err = "unknown instruction: " + Line;
                ok = false;
                break;
            }

            timeline.record(buildSnapshot(call_stack));
        }

        fclose(f);
        return ok;

    }
    catch(...){
        fclose(f);   
        throw;
    }
}


void showing_timeline(Timeline& t){

    cout << "Total Steps " << t.getStepCount() << endl;

    if(t.getStepCount() == 0){

        return;
    }

    TimelineNode* c_st = t.begin();
    int st_ct = 1;

    while(c_st != nullptr){

        Snapshot* snap = c_st->data;
        cout << "Step " << st_ct << " (Depth " << snap->stackDepth << " )" << endl;

        for(int i = snap->stackDepth - 1; i >= 0; i--){

            Frame& frame = snap->callStack[i];
            cout << " " << frame.func_name << " arguments: ";

            for(int j = 0; j < frame.argc; j++){

                cout << " " << frame.argv[j].name << " = " << frame.argv[j].value;
            }

            cout << " locals: ";

            for(int k = 0; k < frame.localCount; k++){

                cout << " " << frame.locals[k].name << " = " << frame.locals[k].value;
            }

            cout << endl;
        }

        c_st = c_st->next;
        st_ct++;
    }
}

// PASS 0x3: SERIALIZE TIMELINE

bool write_str(FILE* f, const string& str)
{
  
    if(str.size() > static_cast<size_t>(MAX_NAME_LENGTH)){

        return false;
    }

    int32_t len = static_cast<int32_t>(str.size());

    if(fwrite(&len, sizeof(int32_t), 1, f) != 1){

        return false;
    }

    if(len > 0 && fwrite(str.data(), 1, len, f) != static_cast<size_t>(len)){

        return false;
    }

    return true;

}

bool write_variable(FILE* f , const Variable& var){

    if(!write_str(f, var.name)){

        return false;
    }


    if(fwrite(&var.value, sizeof(int32_t), 1, f) != 1){

        return false;
    }


    return true;

}

bool write_frame(FILE* f , const Frame& frame){

    if(frame.argc < 0 || frame.argc > MAX_VARS_PER_FRAME){

        cout << "Error!!! ";
        return false;
    }

    if(frame.localCount < 0 || frame.localCount > MAX_VARS_PER_FRAME){

        cout << "Error!!! ";
        return false;
    }

    if(!write_str(f, frame.func_name)){

        cout << "Error!!! Problem while writing " << endl;
        return false;
    }

    if(fwrite(&frame.argc, sizeof(int32_t), 1, f) != 1){

        cout << "Error!!! Problem while writing " << endl;
        return false;
    }

    for(int32_t i = 0; i < frame.argc; i++){

        if(!write_variable(f, frame.argv[i])){

            cout << "Error!!! Problem while writing " << endl;
            return false;
        }
    }

    if(fwrite(&frame.returnLine, sizeof(int32_t), 1, f) != 1){

        cout << "Error!!! couldn't write in file " << endl;
        return false;
    }

    if(fwrite(&frame.localCount, sizeof(int32_t), 1, f) != 1){

        cout << "Error!!! couldn't write in file " << endl;
        return false;
    }

    for(int32_t i = 0; i < frame.localCount; i++){

        if(!write_variable(f, frame.locals[i])){

            cout << "Error!!! couldn't write in file " << endl;
            return false;
        }
    }

    return true;

}


bool write_snapshot(FILE* f , const Snapshot& snapshot){

    if(snapshot.stackDepth < 0 || snapshot.stackDepth > MAX_STACK_DEPTH){

        cout << "Error!!! " << endl;
        return false;
    }

    if(fwrite(&snapshot.stackDepth, sizeof(int32_t), 1, f) != 1){

        cout << "Error!!! Problem while writing " << endl;
        return false;
    }

    for(int32_t i = 0; i < snapshot.stackDepth; i++){

        if(!write_frame(f, snapshot.callStack[i])){
            cout << "Error!!! Problem while writing " << endl;
            return false;
        }
    }

    return true;
}

bool writeTdbg(Timeline &timeline, const char *tdbgPath, string &err)
{

    FILE* f = fopen(tdbgPath, "wb");


    if(f == nullptr){

        err = "server couldn't create session.tdbg";
        return false;
    }

    bool ok = true;

    try{

        int32_t stepCount = timeline.getStepCount();


        if(stepCount < 0 || stepCount > MAX_STEPS){

            throw runtime_error("Invalid timeline step count");
        }

        vector<int64_t> indx(stepCount);


        TTDBHeader header;

        header.magic[0] = 'T';
        header.magic[1] = 'T';
        header.magic[2] = 'D';
        header.magic[3] = 'B';
        header.version = 1;
        header.stepCount = stepCount;
        header.indexOffset = 0;


        writeHeader(f, header);

        TimelineNode* current = nullptr;

        if(stepCount > 0){
        
            current = timeline.begin();
        }

        int32_t step = 0;

        while(current != nullptr && step < stepCount){
        
            long position = ftell(f);

            if(position == -1){
        
                throw runtime_error("Couldn't determine snapshot file offset");
            }

            indx[step] = static_cast<int64_t>(position);

            if(current->data == nullptr || !write_snapshot(f, *current->data)){
        
                throw runtime_error("Failed to write snapshot");
            }

            current = current->next;
            step++;
        }

        if(step != stepCount || current != nullptr){
        
            throw runtime_error("Timeline node count doesn't match step count");
        }

        long indx_position = ftell(f);

        if(indx_position == -1){
        
            throw runtime_error("Couldn't determine index offset");
        }

        header.indexOffset = static_cast<int64_t>(indx_position);

        for(int32_t i = 0; i < stepCount; i++){
        
            if(fwrite(&indx[i], sizeof(int64_t), 1, f) != 1){
        
                throw runtime_error("Failed to write dense index");
            }
        }

        if(fseek(f, 0, SEEK_SET) != 0){
        
            throw runtime_error("Couldn't seek to header");
        }

        writeHeader(f, header);

        if(fflush(f) != 0 || ferror(f)){
        
            throw runtime_error("File write or flush error");
        }
    }
    catch(const exception& e){
        
        err = string("serialization failed: ") + e.what();
        ok = false;
    }

    if(fclose(f) != 0 && ok){
        
        err = "serialization failed: couldn't close session.tdbg";
        ok = false;
    }

    if(!ok){
        
        remove(tdbgPath);
        return false;
    }

    cout << "Successfully created " << tdbgPath << endl;
    return true;

}

volatile sig_atomic_t running = 1;

void onSignal(int sg)

{
    (void)sg;
    running = 0;
}

int createServerListenSocket(int port)
{

    int sock = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);


    if(sock < 0){

        perror("socket");
        exit(1);
    }

    int yes = 1;

    if(setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0){

        perror("setsockopt SO_REUSEADDR");
        close(sock);
        exit(1);
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if(bind(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0){

        perror("bind");
        close(sock);
        exit(1);
    }

    if(listen(sock, SERVER_BACKLOG) < 0){

        perror("listen");
        close(sock);
        exit(1);
    }


    return sock;
}

bool receiveSource(int sock, const char *path, string &err)
{
    char status;

    if(!recvAll(sock, &status, 1)){

        err = "upload failed: nothing received";
        return false;
    }

    if(status != 0){

        err = "upload failed: unexpected status byte";
        return false;
    }

    TrailerChecker checker(status);

    FILE *f = fopen(path, "wb");

    if(f == nullptr){

        err = "server couldn't create source.bin";
        return false;
    }

    vector<char> buf(IO_BUFFER_SIZE);
    vector<char> body(IO_BUFFER_SIZE);
    uint64_t written = 0;
    bool tooBig = false;
    string failure;

    while(true){
        ssize_t r = recv(sock, buf.data(), buf.size(), 0);

        if(r < 0){

            if(errno == EINTR){

                continue;
            }
            else if(errno == EAGAIN || errno == EWOULDBLOCK){

                failure = "upload timed out";
            }else{

                failure = "connection error during upload";
            }

            break;
        }

        if(r == 0){
            break;
        }

        size_t released = checker.feed(buf.data(), static_cast<size_t>(r), body.data());

        if(!tooBig){
            
            written += released;

            if(written > MAX_SOURCE_BYTES){
            
                tooBig = true;
            }
            else if(released > 0 && fwrite(body.data(), 1, released, f) != released){
            
                failure = "server couldn't write source.bin";
                break;
            }
        }
    }

    if(fclose(f) != 0 && failure.empty()){
       
        failure = "server couldn't save source.bin";
    }

    if(!failure.empty()){
       
        remove(path);
        err = failure;
        return false;
    }

    if(tooBig){
       
        remove(path);
        err = "source file is too large (limit " + to_string(MAX_SOURCE_BYTES / (1024 * 1024)) + " MB)";
        return false;
    }

    if(!checker.verify()){
       
        remove(path);
        err = "upload checksum mismatch";
        return false;
    }

    return true;
}

bool sendError(int sock, const string& message)
{
    char status = 1;

    Crc32 crc;
    crc.update(&status, 1);
    crc.update(message.data(), message.size());

    if(!sendAll(sock, &status, 1)){
        return false;
    }

    if(!sendAll(sock, message.data(), message.size())){
        return false;
    }

    char tail[4];
    write_u32(tail, crc.value());

    return sendAll(sock, tail, 4);
}

bool sendTdbgReply(int sock, const char *path)
{
    FILE *f = fopen(path, "rb");

    if(f == nullptr){
       
        sendError(sock, "server couldn't read session.tdbg");
        return false;
    }

    Crc32 crc;
    char status = 0;
    crc.update(&status, 1);
    bool ok = sendAll(sock, &status, 1);

    vector<char> buf(IO_BUFFER_SIZE);

    while(ok){
       
        size_t got = fread(buf.data(), 1, buf.size(), f);

        if(got > 0){
       
            crc.update(buf.data(), got);
            ok = sendAll(sock, buf.data(), got);
        }

        if(got < buf.size()){
       
            if(ferror(f)){
                ok = false;
            }

            break;
        }
    }

    fclose(f);

    if(ok){
       
        char tail[4];
        write_u32(tail, crc.value());
        ok = sendAll(sock, tail, 4);
    }

    return ok;
}

void handleClient(int sock)
{
    timeval tv{};
    tv.tv_sec = SOCKET_TIMEOUT_SEC;
    tv.tv_usec = 0;

    if(setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0){
       
        perror("setsockopt SO_RCVTIMEO");
        return;
    }

    if(setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0){
       
        perror("setsockopt SO_SNDTIMEO");
        return;
    }

        string err;

    try{   
        
        if(!receiveSource(sock, SERVER_SOURCE_FILE.c_str(), err)){
        
            cout << "server rejected: " << err << endl;
            sendError(sock, err);
            return;
        }

        if(!validateProgram(SERVER_SOURCE_FILE.c_str(), err)){
           
            cout << "server rejected: " << err << endl;
            sendError(sock, err);
            return;
        }

        int64_t mainOffset = resolveProgram(SERVER_SOURCE_FILE.c_str(), SERVER_RESOLVE_FILE.c_str(), err);

        if(mainOffset == -1){
           
            cout << "server rejected: " << err << endl;
            sendError(sock, err);
            return;
        }

        Timeline timeline;

        if(!executeProgram(SERVER_RESOLVE_FILE.c_str(), mainOffset, timeline, err)){
            
            cout << "server rejected: " << err << endl;
            sendError(sock, err);
            return;
        }

        if(!writeTdbg(timeline, SERVER_TDBG_FILE.c_str(), err)){
            
            cout << "server rejected: " << err << endl;
            sendError(sock, err);
            return;
        }

        if(!sendTdbgReply(sock, SERVER_TDBG_FILE.c_str())){
        
            cout << "server couldn't deliver the reply" << endl;
        }
    }
    
    catch(const bad_alloc&){
    
        cout << "server out of memory" << endl;
        sendError(sock, "server ran out of memory (program too large)");
    }
    catch(const exception& e){
        
        cout << "server internal error: " << e.what() << endl;
        sendError(sock, string("server internal error: ") + e.what());
    }
}


// main section

int main(int argc, char *argv[])
{
    
    int port = DEFAULT_PORT;

    if(argc >= 2){
    
        char *end = nullptr;
        long v = strtol(argv[1], &end, 10);

        if(end == argv[1] || *end != '\0' || v < 1 || v > 65535){
    
            cout << "Usage: " << argv[0] << " <port>" << endl;
            return 1;
        }

        port = static_cast<int>(v);
    }

    struct sigaction sa{};
    sa.sa_handler = onSignal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    signal(SIGPIPE, SIG_IGN);

    int listenSock = createServerListenSocket(port);
    cout << "[server] listening on port " << port << endl;

    while(running){
    
        int cs = accept(listenSock, nullptr, nullptr);

        if(cs < 0){
    
            if(errno == EINTR){
                continue;
            }

            perror("accept");
            break;
        }

        try{
    
            handleClient(cs);
        }
        catch(const exception& e){
    
            cerr << "[server] unexpected error: " << e.what() << endl;
        }
        catch(...){
    
            cerr << "[server] unknown unexpected error" << endl;
        }

        close(cs);

    
        remove(SERVER_SOURCE_FILE.c_str());
        remove(SERVER_RESOLVE_FILE.c_str());
        remove(SERVER_TDBG_FILE.c_str());
    }

    close(listenSock);
    cout << "[server] bye" << endl;
    return 0;

}