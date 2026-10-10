

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
#include <cstdint>
#include <cstdio>
#include <stdexcept>
using namespace std;

// ---- Constants ----
const int32_t MAX_VARS_PER_FRAME = 16;
const int32_t MAX_STACK_DEPTH = 64;
const int32_t MAX_FUNCS = 128;
const int32_t MAX_TOKENS = MAX_VARS_PER_FRAME + 2; // kW + func_name + upto 16 params/args
const int32_t MAX_PATCHES = MAX_FUNCS * 4;
const uint64_t MAX_SOURCE_BYTES = 15ULL * 1024 * 1024; // sanity cap on the declared file length
const int32_t IO_BUFFER_SIZE = 64 * 1024;                  // fixed buffer for streaming to/from disk
const int32_t SOCKET_TIMEOUT_SEC = 5;                      // TODO: apply as SO_RCVTIMEO so a deadclient can't hang the server forever

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
        count =0;
    }
    void push(const T &val)
    {

        if (count == MAX_STACK_DEPTH){
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
       
      if(count == 0 || top == nullptr ){
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
        if(count == 0 || top == nullptr ){
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

        int32_t  indx = 0;
        Node* c_n = top; // current node 

        while(c_n != nullptr && indx < maxLen){
            out[indx] = c_n -> data;
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
        
        if (head == nullptr){
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

Timeline::~Timeline(){
    TimelineNode* c_n = head;
    while(c_n != nullptr){
      TimelineNode* n = c_n->next;
      delete c_n -> data;
      delete c_n;
      c_n = n;

    }
}

struct TTDBHeader
{
    char magic[4]; // "TTDB"
    int32_t version;
    int32_t stepCount;
    int64_t indexOffset;
};
void writeHeader(FILE *f, const TTDBHeader &h)
{
    fwrite(h.magic, 1, 4, f);
    fwrite(&h.version, sizeof(int32_t), 1, f);

    // placeholder for other two data members
}

// resolve.bin - bookkeeping
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



// PASS 0x0: READING source.bin + VALIDITY CHECK

bool is_space(char ch){
   
    if (ch == ' ' || ch == '\t' || ch == '\r' ){
        return true;
    }
    return false;
}

bool readSourceLine(ifstream &in, string &out)
{
    // reads the next nonblank line

    string line;
    while(getline(in , line)){
        
        int line_length  = line.length();
        int indx =0;
      
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
    int indx =0;
    int len = line.length();

    
        
    while (indx < len && is_space(line[indx])){
          
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
    int indx =0;
    int len = line.length();

  
    while(indx < len && is_space(line[indx])){
           
        indx++; 
    }

    while(indx < len && !is_space(line[indx])){
        
        indx++; 
    }
       
       
    while (indx < len && is_space(line[indx])){
            
        indx++; 
    }

    while(indx < len && !is_space(line[indx])){

        second_word = second_word + line[indx];
        indx++;  
    }

    return second_word;
}
bool validateProgram(const char *sourcePath)
{
    // for each func defined there should be exactly one func_end and no nested funcs allowed - 

    ifstream fin(sourcePath);
    if(!fin){
        cout << "Error!!!!  Couldn't open the file " << endl;
        return false;
    } 

    string Line;
    bool inside = false ;
    while (readSourceLine(fin ,Line)){

       
        string word =  firstWord(Line);
       
        
        if(word == "func"){
            if(inside == true){
                return false;
            }else{
                inside = true;
            }
        }
        else if(word == "func_end"){
           
            if(!inside){
                return false;
            }else{
                inside = false;
            }
        }



    }

    return !inside;
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
    
    
    if(fwrite(&offsetField, sizeof(int64_t),1 , f)!=1){

        cout << "Error writing offset field" << endl;
        return -1;

    }  // storing value of offset field using 8 bytes
    
    int32_t string_size = static_cast<int32_t>(text.size());
    if(fwrite(&string_size , sizeof(int32_t),1,f)!= 1){

        cout << "Error writing offset field" << endl;
        return -1;


    }

    if(fwrite(text.data(), 1, text.size(), f) != text.size()){

        cout << "Error writing string" << endl;
        return -1;
    }


   return record_start;
}

int64_t readResolveRecord(FILE *f, string &outText){
    // reads one record at the current position and advances past it, returns the offset field - the raw line text comes back untouched in outText.
   int64_t offset_field;
    if (fread(&offset_field, sizeof(int64_t), 1, f) != 1){
        return -1;
    }

    int32_t string_size;
    if (fread(&string_size , sizeof(int32_t),1 , f)!= 1){
        return -1;
    }

    if(string_size < 0){

        cout << "Invalid string size" << endl;
        return -1;
      
    }

    outText.resize(string_size);
    if(string_size > 0){
        if(fread(&outText[0] , 1, string_size, f)!= static_cast<size_t>(string_size)){
            
            cout << "Error reading string "<<endl;
            return -1; 
        }
    }
   return offset_field;
}
int64_t resolveProgram(const char *sourcePath, const char *resolveBinPath)
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
    FILE* f = fopen(resolveBinPath,"wb");

    if(!fin || f == nullptr){

        if(f != nullptr){
            fclose(f);
        }
        cout << " File couldn't be opened "<<endl;
        return -1;
    }
    string Line;
    while(readSourceLine(fin ,Line )){
        
        int64_t offset_field = ftell(f);
        if(offset_field == -1){

            cout << "Error!!! getting file position" << endl;
            fclose(f);
            return -1;
        }


        int64_t r_st_p = writeResolveRecord(f,offset_field,Line); // r_st_p is records starting point 
         if(r_st_p == -1){

            cout << "Error!!! couldn't write file " << endl;
            fclose(f);
            return -1;
        }


        string word = firstWord(Line);
        
        if (word == "func"){

            if(funcCount >= MAX_FUNCS){
                cout << "Too many functions"<<endl;
                fclose(f);
                return -1;
            }
            
            string func_name = secondWord(Line);

            if(func_name == ""){
                cout << "Function has no name"<<endl;
                fclose(f);
                return -1;
            }

            funcArray[funcCount].funcName   = func_name;
            funcArray[funcCount].byteOffsetInResolveBin = r_st_p;
            funcCount++;
        }else if (word == "call"){
            
            if (patchCount >= MAX_PATCHES){

                cout << " Too many calls "<< endl;
                fclose(f);
                return -1;

            }

            string func_name = secondWord(Line);
            
            if(func_name == ""){
                cout << "Function has no name"<<endl;
                fclose(f);
                return -1;
            }
            patches[patchCount].targetFuncName = func_name;
            patches[patchCount].byteOffsetOfOffsetField = r_st_p;
            patchCount++;
        }
       
    }

    for(int32_t i =0; i < patchCount ; i++){
        
        string tar_func = patches[i].targetFuncName;
        bool found = false;

        for(int32_t j =0; j < funcCount;j++){
        
            if(tar_func == funcArray[j].funcName){

                int64_t offset_field = patches[i].byteOffsetOfOffsetField;
                if (fseek(f,offset_field,SEEK_SET) != 0){

                    cout << "Error!!! seeking to Call offset field "<<endl;
                    fclose(f);
                    fin.close();
                    return -1;
                }
              
                int64_t target_offset = funcArray[j].byteOffsetInResolveBin;
                if (fwrite(&target_offset, sizeof(int64_t),1,f) != 1){
                    
                    cout << "Error!!! writing patched Call offset "<<endl;
                    fclose(f);
                    fin.close();
                    return -1;
                }

                found = true;
                break;

            }
        }

        if(!found){

            cout << "Error!!!! call to undefined function" << tar_func << endl;
            fclose(f);
            return -1;
        }

    }

    fclose(f);

    for (int32_t j =0; j<funcCount;j++){
        if(funcArray[j].funcName == "main"){
            return funcArray[j].byteOffsetInResolveBin;
            
        }
    }

    cout << "Error!!! no main function "<<endl;
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
    // first word is always a instruction keyword
    // instruction set = [func, func_end, call, set, add, sub, mul and div]
    // next word is identifier like name of a function, variable name
    // after identifier all are the params/arg, space separated


    int size = line.length();
    int indx = 0;
    int ct =0;

    while(indx < size && is_space(line[indx])){

        indx++;
    }

    if (indx >= size){
        return 0;
    }
    string first_word;
    
    while(indx < size && !is_space(line[indx])){
        first_word += line[indx];
        indx++;
    }
    tokens[ct].text = first_word;
    tokens[ct].type = KEYWORD;
    ct++;

    while(indx < size && is_space(line[indx])){
        indx++;
    }

    if(indx >= size || ct>= maxTokens){
        return ct;
    }

    string second_word;
   
    while(indx < size && !is_space(line[indx])){
        second_word += line[indx];
        indx++;
       
    }
    tokens[ct].text = second_word;
    tokens[ct].type = IDENTIFIER;
    ct++;

   while(indx < size && ct < MAX_TOKENS){

        while(indx < size && is_space(line[indx])){
           indx++;
        }

        if(indx >= size){
            break;
        }

        string params ;

        while(indx <size && !is_space(line[indx])){

            params += line[indx];
            indx++;
        }

        tokens[ct].text = params;
        tokens[ct].type = PARAM;
        ct++;
    }

    return ct;

}
Snapshot *buildSnapshot(Stack<Frame> &callStack)
{
    // build the snapshot based on the callStack given

    Snapshot* snapshot = new Snapshot();

    snapshot->stackDepth = callStack.depth();

    callStack.snapshot_into(snapshot->callStack, MAX_STACK_DEPTH);

    return snapshot;

}

// helper functions

Variable* find_var(Frame& frame , const string& var_name){

   int count = frame.argc; 
   for(int i = 0;i< count;i++){
       
        if (frame.argv[i].name == var_name){
           return &frame.argv[i];
        }
    }

   for(int i =0; i<frame.localCount;i++){
    
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

    while(indx < size ){
        if(str[indx] < '0' || str[indx] > '9'){
            return false;
        }
        indx++;
    }

    return true;
}

int32_t str_to_num(const string& str){
  
    int indx = 0;
    bool neg = false;
    int32_t num = 0;

    if(str[0] == '-'){
       neg = true;
       indx = 1;
    }

    int size = str.length();

    while(indx < size ){
      int dig = str[indx] -'0';
      num = num* 10 + dig;
      indx++;
    }

    if(neg == true){
      num = -num;
    }

   return num;

}

bool getting_value(Frame& frame , string& str , int32_t& res){

    if(is_num(str)){
       
        res = str_to_num(str);
        return true;
       
    }

    Variable* var = find_var(frame , str);
    if(var == nullptr){
        return false; 
    }

    res = var->value;
    return true;
}


void executeProgram(const char *resolveBinPath, int64_t mainOffset, Timeline &timeline)
{
    // initialize the call stack
    // make the main frame
    // push main frame on the call stack

    // implementation:
    // execute line by line, and according to the keyword perform action

    FILE* f = fopen(resolveBinPath ,"rb");

    if(f == nullptr){
           
        cout << "Error!!! Couldn't open file " << resolveBinPath << endl;
        return; 
    }
    
    Stack<Frame> call_stack;
    Token Tokens[MAX_TOKENS];
    string Line;

    if(fseek(f,mainOffset ,SEEK_SET)!=0){

        cout << "Error!!! Couldn't seek to main funcion "<<endl;
        fclose(f);
        return;
    }

    int64_t main_offset = readResolveRecord(f,Line);
    if(main_offset == -1){

        cout << "Error!!! Couldn't read main function "<<endl;
        fclose(f);
        return;
    }

    int token_ct = tokenizeLine(Line , Tokens ,MAX_TOKENS);

    if(token_ct < 2){

        cout << "Error!!! Invalid main function record" << endl;
        fclose(f);
        return;
    }


    Frame main_frame;
    main_frame.func_name = Tokens[1].text;
    main_frame.argc = token_ct -2;
    main_frame.localCount = 0;
    main_frame.returnLine = -1;

    for(int i =0;i<main_frame.argc ; i++){

        main_frame.argv[i].name = Tokens[i+2].text;
        main_frame.argv[i].value =0;
    }

    call_stack.push(main_frame);

    timeline.record(buildSnapshot(call_stack));

    while(!call_stack.isEmpty()){

        int64_t re_ps = ftell(f); // record position

        if(re_ps == -1){

            cout << "Error!!! Couldn't determine current file position "<< endl;
            break;
        }

        int64_t Offset_Field = readResolveRecord(f , Line);
        
        if(Offset_Field == -1){

            cout << "Error!!! unexpected end of the file "<< endl;
            break;
        }
        
        token_ct = tokenizeLine(Line , Tokens ,MAX_TOKENS);

        if(token_ct <=0){

            cout << "Error!!! Invalid instruction " << Line << endl;
            break;
        }

        string Keyword = Tokens[0].text;
        Frame& c_at = call_stack.peek();

        if(Keyword == "set"){

            int32_t val = 0;

            if(token_ct <3 || !getting_value(c_at,Tokens[2].text , val)){

                cout << "Error!!! Setting value " << endl;
                break;
            }

            Variable* var = find_var(c_at , Tokens[1].text);

            if(var == nullptr){

                if(c_at.localCount >= MAX_VARS_PER_FRAME){
                     
                    cout << " Too many variables in " << c_at.func_name << " function" << endl;
                    break;
                }

                var = &c_at.locals[c_at.localCount];
                var->name = Tokens[1].text;
                c_at.localCount ++;
           
            }

            var->value = val;

        }
        else if( Keyword == "div" || Keyword == "mul" || Keyword == "add" || Keyword == "sub"){

            if(token_ct <3){

                cout << "Error!!! " << Keyword << " reqires 2 operands " << endl;
                break;
            }

            Variable* res = find_var(c_at, Tokens[1].text);
            
            int32_t op = 0; // operand

            if(res == nullptr || !getting_value(c_at ,Tokens[2].text,op)){

                cout << "Error!!! undefined variable in " << Line << endl;
                break;
            }

            if(Keyword == "div"){

                if(op == 0){

                    cout << "Error!!! (unallowed) division by 0 in "<< Line << endl;
                    break;
                }

                res->value = res->value /op;
            }
            else if(Keyword == "mul"){

                res->value = res->value * op;
            }
            else if(Keyword == "sub"){

                res->value = res->value - op;
            }
            else if(Keyword == "add"){

                res->value = res->value + op;
            }
        }
        else if(Keyword == "call"){

            int32_t arg_ct = token_ct - 2;
            int32_t arg_val[MAX_VARS_PER_FRAME];

            if(arg_ct > MAX_VARS_PER_FRAME){

                cout << " Error!!! too many arguments "<< endl;
                break;
            }

            bool g_val = true; // got value

            for(int i = 0; i < arg_ct ;i++){
                
                if(!getting_value(c_at , Tokens[i+2].text,arg_val[i])){

                    cout << "Error!!! " << Tokens[i+2].text << " is not defined" << endl;
                    g_val = false;
                    break;
                }
            }

            if(g_val == false){
                break;
            }

            if(fseek(f,Offset_Field,SEEK_SET)!= 0){

                cout << "Error!!! Couldn't seek to called function " << endl;
                break;
            }

            if(readResolveRecord(f, Line) == -1){

                cout << "Error!!! Couldn't read called function "<<endl;
                break;
            }

            int hdr_ct = tokenizeLine(Line , Tokens , MAX_TOKENS); // header count

            if(hdr_ct < 2){

                cout << "Error!!! Invalid function header " <<endl;
                break;

            }

            int par_ct = hdr_ct - 2; // parameter counter

            if(par_ct != arg_ct){

                cout << "Error!!! " << Tokens[1].text << " needs " << par_ct  << " arguments but it got " << arg_ct << endl;
                break;
            }

            Frame frame; 
            frame.func_name = Tokens[1].text;
            frame.argc = par_ct;
            frame.localCount = 0;
            frame.returnLine = re_ps;

            for(int i =0; i < par_ct ; i++){
                
                frame.argv[i].name = Tokens[i+2].text;
                frame.argv[i].value = arg_val[i];
            }
            
            if(call_stack.depth() >=MAX_STACK_DEPTH){

                cout << " ERROR!!! STACK IS FULL " << endl;
                break;
            }


            call_stack.push(frame);

        }else if(Keyword == "func_end"){

            Frame completed = call_stack.pop();

            if(completed.returnLine != -1){

                if(fseek(f,completed.returnLine , SEEK_SET) != 0){
                    
                    cout << "Error!!! Couldn't return to caller " << endl;
                    break;
                }

                if(readResolveRecord(f,Line) == -1){

                    cout << "Error!!! Couldn't read caller instructions "<< endl;
                    break;
                }

                int caller_token_ct = tokenizeLine(Line , Tokens ,MAX_TOKENS);

                if(caller_token_ct < completed.argc + 2){

                   cout << "Error!!! Invalid caller instructions "<< endl;
                    break;
                }

                if(call_stack.isEmpty()){

                    cout << "Error!!! No caller frame exists "<< endl;
                    break;
                }

                Frame& caller_frame = call_stack.peek();

                for (int i = 0; i < completed.argc; i++){

                    Variable* var = find_var(caller_frame, Tokens[i+2].text);

                    if(var != nullptr){

                        var->value = completed.argv[i].value;
                    }

                }
            }

        }else{

            cout << "Unknown Instructions " << Line << endl;
            break;
        }
        timeline.record(buildSnapshot(call_stack));

    }

    fclose(f);

}

void showing_timeline(Timeline& t){

    cout << "Total Steps " << t.getStepCount() << endl;
    if(t.getStepCount() == 0){
       
        return;
    }

    TimelineNode* c_st = t.begin(); 

    int st_ct = 1;
    
    while(c_st!= nullptr){

        Snapshot* snap = c_st->data;

        cout << "Step " << st_ct << " (Depth " << snap->stackDepth << " )" << endl;

        for (int i = snap->stackDepth - 1; i >= 0 ;i--){

            Frame& frame = snap->callStack[i];

            cout << " " << frame.func_name << " arguments: " ;

            for(int j = 0 ; j < frame.argc; j++){

                cout << " " << frame.argv[j].name << " = " << frame.argv[j].value;
            }

            cout << " locals: ";

             for(int k = 0 ; k < frame.localCount; k++){

                cout << " " << frame.locals[k].name << " = " << frame.locals[k].value;
            }
            cout << endl;

        }

        c_st = c_st->next;
        st_ct++;
    }
}

// PASS 0x3: SERIALIZE TIMELINE
void writeTdbg(Timeline &timeline, const char *tdbgPath)
{
    // placeholder for header
    // index array of the size of stepcount from the timeline
    // placing each snapshot in the file while maintaining the index(starting point of each nth snapshot)
    // after timeline add the index array i the file
    // update the header
}
// main section

int32_t main()
{

    if (!validateProgram("source.bin"))
    {
        // send an error response instead of a .tdbg file
        return 1;
    }

    int64_t mainOffset = resolveProgram("source.bin", "resolve.bin");

    if (mainOffset == -1){
        // throw error 
        return 1;
    }
    Timeline timeline;
    executeProgram("resolve.bin", mainOffset, timeline);

    writeTdbg(timeline, "session.tdbg");

    return 0;
}