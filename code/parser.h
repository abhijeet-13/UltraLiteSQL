#ifndef parser_h
#define parser_h

#include <iostream>
#include <unordered_map>
#include <vector>
#include <sstream>
#include "abhisql.h"
using namespace std;


class as_parser{
    string prompt;
    string command;
    unordered_map<string, vector<string> > command_ex_keywords;
    vector<string> ex_keywords;
    vector<string> valid_data_types;
    Abhi_sql_engine engine;
    
public:
    as_parser(){
        prompt = string("\nultralitesql> ");
        
        // keywords
        ex_keywords.clear();
        command_ex_keywords["show"] = vector<string>();
        command_ex_keywords["show"].push_back("tables");
        
        command_ex_keywords["create1"] = vector<string>();
        command_ex_keywords["create1"].push_back("table");
        
        command_ex_keywords["create2"] = vector<string>();
        command_ex_keywords["create2"].push_back("row_id");
        command_ex_keywords["create2"].push_back("int");
        command_ex_keywords["create2"].push_back("primary");
        command_ex_keywords["create2"].push_back("key");
        
        command_ex_keywords["notnull"] = vector<string>();
        command_ex_keywords["notnull"].push_back("not");
        command_ex_keywords["notnull"].push_back("null");
        
        command_ex_keywords["drop"] = vector<string>();
        command_ex_keywords["drop"].push_back("table");
        
        command_ex_keywords["insert1"] = vector<string>();
        command_ex_keywords["insert1"].push_back("into");
        command_ex_keywords["insert1"].push_back("table");
        
        command_ex_keywords["insert2"] = vector<string>();
        command_ex_keywords["insert2"].push_back("values");
        
        command_ex_keywords["update"] = vector<string>();
        command_ex_keywords["select"] = vector<string>();
        
        
        //Valid data types
        valid_data_types.push_back("tinyint");
        valid_data_types.push_back("smallint");
        valid_data_types.push_back("int");
        valid_data_types.push_back("bigint");
        valid_data_types.push_back("real");
        valid_data_types.push_back("double");
        valid_data_types.push_back("datetime");
        valid_data_types.push_back("date");
        valid_data_types.push_back("text");
    }
    
    
    // Split based on character
    vector<string> split(string str, char sp){
        vector<string> out;
        stringstream str_s(str);
        while(!str_s.eof()){
            string tmp;
            getline(str_s, tmp, sp);
            out.push_back(tmp);
        }
        return out;
    }
    
    
    
    // Pass list of words that need to be passed
    bool pass_words(stringstream &ss, vector<string> &words, char delim = ' '){
        for(int i = 0; i < words.size(); i++){
            char current_delim = (i == words.size() - 1) ? delim : ' ';
            if(words[i] != extract_word(ss, current_delim))
                return false;
        }
        return true;
    }
    
    
    // Reads leading ws
    // Stops before either first ws or first delim
    // Returns "" if delim was not encountered and entire stream was read
    string extract_word(stringstream &ss, char delim = ' '){
        string word("");
        ss >> ws;           // eat leading whitespace
        
        // read till end of file
        while(ss.peek() != EOF){
            // done reading if any of these happens
            if(isspace(ss.peek()) || ss.peek() == delim)
                break;
            word = word + char(ss.get());
        }
        if(ss.peek() == EOF && delim != ' ')
            return string("");
        
        ss >> ws;           // eat trailing whitespace
        //cout << "\nExtracted #" << word << "#\n";
        return word;
    }
    
    
    
    string extract_value(string text){
        size_t first_valid = text.find_first_not_of(" \n\t");
        if(first_valid == string::npos) return "";
        
        // remove leading and trailing whitespace
        text = text.substr(first_valid);
        text = text.substr(0, text.size() - string(text.rbegin(), text.rend()).find_first_not_of(" \n\t"));
        
        if(text[0] == '\''){
            if(text[text.size() - 1] == '\''){
                return text.substr(1, text.size() - 2);
            }
            else{
                size_t value_end = text.find_last_of("\'");
                if(value_end == string::npos){
                    cout << "[Error] Closing quote not found for value\n";
                    return "";
                }
                cout << "[Warning] Trailing text after closing of quotes ignored\n";
                return text.substr(0, value_end);
            }
        }
        
        size_t first_ws = text.find_first_of(" \n\t");
        if(first_ws == string::npos){
            return text;
        }
        else{
            cout << "[Warning] Trailing text after first string expression within value ignored\n";
            return text.substr(0, first_ws);
        }
    }
    
    
    
    
    void launch(){
        cout << "Launching Abhi SQL...\n";
        while(true){
            // Display prompt and take the command as input until the user hits return
            cout << prompt;
            getline(cin, command, ';');
            transform(command.begin(), command.end(), command.begin(), ::tolower);
            
            size_t command_begin = command.find_first_of("abcdefghijklmnopqrstuvwxyz");
            if(command_begin == string::npos){
                cout << "\n";
                continue;
            }
            command = command.substr(command_begin);
            
            // Process the command
            if(!process(command))
                break;
        }
    }
    
    bool process(string command){
        //size_t first_word_begin = command.find_first_not_of(" ");
        //size_t first_word_end = command.find_first_of(" ");
        
        stringstream ss(command);
        string action = extract_word(ss);
        //cout << "Action was #" << action << "#\n";
        
        if(action == string("exit")){
            cout << "Bye!\n";
            return false;
        }
        else if(action == "show"){
            if(!pass_words(ss, command_ex_keywords["show"])){
                cout << "Incorrect syntax. Did you mean \'SHOW TABLES\'?\n";
                return true;
            }
            engine.show_tables();
        }
        else if(action == "create"){
            // parse initial syntax
            if(!pass_words(ss, command_ex_keywords["create1"])){
                cout << "[Syntax error] Did you mean \'CREATE TABLE table_name (...)\'?\n";
                return true;
            }
            
            // Extract table name
            string table_name = extract_word(ss, '(');
            if(table_name == string("")){
                cout << "[Error] Table name not provided or left parenthesis is missing\n";
                return true;
            }
            
            ss.get();       // eat left parenthesis
            string within_paren;
            getline(ss, within_paren, ')');
            if(ss.eof()){
                cout << "[Error] Right matching parenthesis not found\n";
                return true;
            }
            vector<string> column_info = split(within_paren, ',');
            
            // Read remaining columns info
            vector<column_type> columns;
            for(int i = 0; i < column_info.size(); i++){
                // read in to a stream
                stringstream css(column_info[i]);
                
                // parse first line
                if(i == 0){
                    if(!pass_words(css, command_ex_keywords["create2"])){
                        cout << "[Syntax error] First column should always be 'row_id int primary key'\n";
                        return true;
                    }
                    continue;
                }
                
                column_type ct;
                
                // column name
                ct.column_name = extract_word(css);
                if(ct.column_name == ""){
                    cout << "[Error] No column name provided for column number " << i + 1 << "\n";
                    return true;
                }
                
                // column data type
                ct.data_type = extract_word(css);
                if(ct.data_type == ""){
                    cout << "[Error] No column data type provided for column number " << i + 1 << "\n";
                    return true;
                }
                bool valid_data_type = false;
                for(int i = 0; i < valid_data_types.size(); i++){
                    if(valid_data_types[i] == ct.data_type){
                        valid_data_type = true;
                        break;
                    }
                }
                if(!valid_data_type){
                    cout << "[Error] \'" << ct.data_type << "\' is not a valid data type\n";
                    return true;
                }
                
                // not null
                if(!css.eof()){
                    if(pass_words(css, command_ex_keywords["notnull"])){
                        ct.not_null = true;
                    }
                    else{
                        cout << "[Error] Invalid column specifier. Did you mean \'not null\'\n";
                        return true;
                    }
                }
                columns.push_back(ct);
            }
            
            engine.create_table(table_name, columns);
        }
        else if(action == "drop"){
            if(!pass_words(ss, command_ex_keywords["drop"])){
                cout << "Incorrect syntax. Did you mean \"DROP TABLES table_name;\"?\n";
                return true;
            }
            string table_name = extract_word(ss);
            engine.drop_table(table_name);
        }
        else if(action == "insert"){
            // parse initial syntax
            if(!pass_words(ss, command_ex_keywords["insert1"])){
                cout << "[Syntax error] Correct syntax is \"INSERT INTO TABLE table_name [columns] values(..)\"?\n";
                return true;
            }
            string table_name = extract_word(ss, '(');
            
            
            // find which params to insert
            vector<string> insert_columns;
            if(ss.peek() == '('){
                ss.get();
                string within_paren;
                getline(ss, within_paren, ')');
                vector<string> parts = split(within_paren, ',');
                for(int i = 0; i < parts.size(); i++){
                    stringstream iss(parts[i]);
                    insert_columns.push_back(extract_word(iss));
                }
            }
            
            // parse later syntax
            string values_keyword = extract_word(ss, '(');
            if(values_keyword != "values"){
                cout << "[Syntax error] Use keyword VALUES in place of \'" << values_keyword << "\'\n";
                return true;
            }
            
            // find which values to insert
            vector<string> insert_values;
            if(ss.peek() == '('){
                ss.get();
                string within_paren;
                getline(ss, within_paren, ')');
                
                if(ss.eof()){
                    cout << "[Syntax error] Missing closing parenthesis\n";
                    return true;
                }
                
                vector<string> parts = split(within_paren, ',');
                for(int i = 0; i < parts.size(); i++){
                    //stringstream iss(parts[i]);
                    //string column_val = extract_word(iss);
                    string column_val = extract_value(parts[i]);
                    if(column_val == ""){
                        cout << "[Error] Missing value for a column\n";
                        return true;
                    }
                    insert_values.push_back(column_val);
                }
            }
            if(insert_values.size() == 0){
                cout << "[Error] No values were specified for insertion\n";
                return true;
            }
            
            if(insert_columns.size() > 0 && insert_values.size() != insert_columns.size()){
                cout << "[Error] Mismatched number of columns and values\n";
                return true;
            }
            
            // insert values into table
            engine.insert(table_name, insert_values, insert_columns);
        }
        else if(action == "update"){
            string table_name = extract_word(ss);
            
            string set_keyword = extract_word(ss);
            if(set_keyword != "set"){
                cout << "[Error]: Incorrect syntax. Did you forget the SET keyword?\n";
                return true;
            }
            
            size_t where_loc = command.find("where ");
            string column_name = extract_word(ss, '=');
            ss.get();
            string value = extract_word(ss);
            value = extract_value(value);
            
            
            string where = "";
            where_condition cond;
            if(where_loc != string::npos){
                where = command.substr(where_loc);
            }
            if(where.size() > 0){
                // eat where keyword
                where = where.substr(6);
                
                // find comparison operator
                size_t loc = min(where.find_first_of("=!<>"), where.find(" is "));
                if(loc == string::npos){
                    cout << "[Error] No valid comparison operator found\n";
                    return true;
                }
                
                // extract comparison variable
                ss.str(where.substr(0, loc));
                string comp_variable = extract_word(ss);
                if(comp_variable == ""){
                    cout << "[Error] No column name specified before the comparison operator\n";
                    return true;
                }
                cond.column_name = comp_variable;
                
                // extract remaining values
                where = where.substr(loc);
                switch(where[0]){
                    case '=':{
                        cond.comp_code = 0;
                        cond.value_is_null = false;
                        
                        cond.value = extract_value(where.substr(1));
                        if(cond.value == ""){
                            return true;
                        }
                        break;
                    }
                    case '!':{
                        if(where[1] != '='){
                            cout << "[Error] Syntax error for comparison operator, try using \'var != value\'\n";
                            return true;
                        }
                        cond.comp_code = 1;
                        cond.value_is_null = false;
                        
                        cond.value = extract_value(where.substr(2));
                        if(cond.value == ""){
                            return true;
                        }
                        break;
                    }
                    case '<':{
                        if(where[1] == '='){
                            cond.comp_code = 4;
                            cond.value_is_null = false;
                            
                            cond.value = extract_value(where.substr(2));
                            if(cond.value == ""){
                                return true;
                            }
                        }
                        else{
                            cond.comp_code = 2;
                            cond.value_is_null = false;
                            
                            cond.value = extract_value(where.substr(1));
                            if(cond.value == ""){
                                return true;
                            }
                        }
                        break;
                    }
                    case '>':{
                        if(where[1] == '='){
                            cond.comp_code = 5;
                            cond.value_is_null = false;
                            
                            cond.value = extract_value(where.substr(2));
                            if(cond.value == ""){
                                return true;
                            }
                        }
                        else{
                            cond.comp_code = 3;
                            cond.value_is_null = false;
                            
                            cond.value = extract_value(where.substr(1));
                            if(cond.value == ""){
                                return true;
                            }
                        }
                        break;
                    }
                    case ' ':{
                        cond.value_is_null = true;
                        cond.value = "null";
                        
                        stringstream where_ss(where);
                        string is_keyword = extract_word(where_ss);
                        string value_keyword = extract_word(where_ss);
                        if(value_keyword == "null"){
                            cond.comp_code = 6;
                        }
                        else if(value_keyword == "not"){
                            value_keyword = extract_word(where_ss);
                            if(value_keyword == "null"){
                                cond.comp_code = 7;
                            }
                            else{
                                cout << "[Error] Did you mean 'is not null'?\n";
                                return true;
                            }
                        }
                        else{
                            cout << "[Error] Did you mean 'is null'?\n";
                            return true;
                        }
                        break;
                    }
                    default:{
                        cout << where;
                        cout << "[Error] Comparison operator not recognized\n";
                        return true;
                    }
                }
            }
            
            engine.update_records(table_name, column_name, value, cond);
            
        }
        else if(action == "select"){
            
            // Obtain select
            size_t curr_loc = 6;
            size_t from_loc = command.find("from");
            if(from_loc == string::npos || !isspace(command[from_loc - 1]) || !isspace(command[from_loc + 4])){
                cout << "[Error] Invalid syntax, could not find the 'from' keyword\n";
                return true;
            }
            string select = command.substr(curr_loc, from_loc - curr_loc);
            
            // Obtain from
            size_t where_loc = command.find("where ");
            string from = command.substr(from_loc, where_loc - from_loc);
            //cout << "from #" << from << "#\n";
            
            // Obtain where
            string where = "";
            if(where_loc != string::npos){
                where = command.substr(where_loc);
            }
            //cout << "where #" << where << "#\n";
            
            
            // Parse select, obtain columns
            vector<string> proj_columns = split(select, ',');
            for(int i = 0; i < proj_columns.size(); i++){
                stringstream tmp(proj_columns[i]);
                string column_name_word = extract_word(tmp);
                // TODO: "", special chars except *, repeated columns - handle all
                proj_columns[i] = column_name_word;
            }
            if(proj_columns.size() == 0 || proj_columns[0] == ""){
                cout << "[Error] No projected columns specified in command\n";
                return true;
            }
            
            // Parse from, obtain table_name
            stringstream from_ss(from);
            string from_keyword = extract_word(from_ss);
            string table_name = extract_word(from_ss);
            if(table_name == ""){
                cout << "[Error] No table to select records from\n";
                return true;
            }
            
            // Parse where condition
            where_condition cond;
            if(where.size() > 0){
                // eat where keyword
                where = where.substr(6);
                
                // find comparison operator
                size_t loc = min(where.find_first_of("=!<>"), where.find(" is "));
                if(loc == string::npos){
                    cout << "[Error] No valid comparison operator found\n";
                    return true;
                }
                
                // extract comparison variable
                ss.str(where.substr(0, loc));
                string comp_variable = extract_word(ss);
                if(comp_variable == ""){
                    cout << "[Error] No column name specified before the comparison operator\n";
                    return true;
                }
                cond.column_name = comp_variable;
                
                // extract remaining values
                where = where.substr(loc);
                switch(where[0]){
                    case '=':{
                        cond.comp_code = 0;
                        cond.value_is_null = false;
                        
                        cond.value = extract_value(where.substr(1));
                        if(cond.value == ""){
                            return true;
                        }
                        break;
                    }
                    case '!':{
                        if(where[1] != '='){
                            cout << "[Error] Syntax error for comparison operator, try using \'var != value\'\n";
                            return true;
                        }
                        cond.comp_code = 1;
                        cond.value_is_null = false;
                        
                        cond.value = extract_value(where.substr(2));
                        if(cond.value == ""){
                            return true;
                        }
                        break;
                    }
                    case '<':{
                        if(where[1] == '='){
                            cond.comp_code = 4;
                            cond.value_is_null = false;
                            
                            cond.value = extract_value(where.substr(2));
                            if(cond.value == ""){
                                return true;
                            }
                        }
                        else{
                            cond.comp_code = 2;
                            cond.value_is_null = false;
                            
                            cond.value = extract_value(where.substr(1));
                            if(cond.value == ""){
                                return true;
                            }
                        }
                        break;
                    }
                    case '>':{
                        if(where[1] == '='){
                            cond.comp_code = 5;
                            cond.value_is_null = false;
                            
                            cond.value = extract_value(where.substr(2));
                            if(cond.value == ""){
                                return true;
                            }
                        }
                        else{
                            cond.comp_code = 3;
                            cond.value_is_null = false;
                            
                            cond.value = extract_value(where.substr(1));
                            if(cond.value == ""){
                                return true;
                            }
                        }
                        break;
                    }
                    case ' ':{
                        cond.value_is_null = true;
                        cond.value = "null";
                        
                        stringstream where_ss(where);
                        string is_keyword = extract_word(where_ss);
                        string value_keyword = extract_word(where_ss);
                        if(value_keyword == "null"){
                            cond.comp_code = 6;
                        }
                        else if(value_keyword == "not"){
                            value_keyword = extract_word(where_ss);
                            if(value_keyword == "null"){
                                cond.comp_code = 7;
                            }
                            else{
                                cout << "[Error] Did you mean 'is not null'?\n";
                                return true;
                            }
                        }
                        else{
                            cout << "[Error] Did you mean 'is null'?\n";
                            return true;
                        }
                        break;
                    }
                    default:{
                        cout << where;
                        cout << "[Error] Comparison operator not recognized\n";
                        return true;
                    }
                }
            }
            
            engine.display_records(engine.select_records(engine.get_all_records(table_name, true), table_name, cond, proj_columns));
        }
        else if(action == "delete"){
            string from_keyword = extract_word(ss);
            if(from_keyword != "from"){
                cout << "[Error]: Incorrect syntax. Did you forget the FROM keyword?\n";
                return true;
            }
            string table_name = extract_word(ss);
            
            size_t where_loc = command.find("where ");
            
            string where = "";
            where_condition cond;
            if(where_loc != string::npos){
                where = command.substr(where_loc);
            }
            if(where.size() > 0){
                // eat where keyword
                where = where.substr(6);
                
                // find comparison operator
                size_t loc = min(where.find_first_of("=!<>"), where.find(" is "));
                if(loc == string::npos){
                    cout << "[Error] No valid comparison operator found\n";
                    return true;
                }
                
                // extract comparison variable
                ss.str(where.substr(0, loc));
                string comp_variable = extract_word(ss);
                if(comp_variable == ""){
                    cout << "[Error] No column name specified before the comparison operator\n";
                    return true;
                }
                cond.column_name = comp_variable;
                
                // extract remaining values
                where = where.substr(loc);
                switch(where[0]){
                    case '=':{
                        cond.comp_code = 0;
                        cond.value_is_null = false;
                        
                        cond.value = extract_value(where.substr(1));
                        if(cond.value == ""){
                            return true;
                        }
                        break;
                    }
                    case '!':{
                        if(where[1] != '='){
                            cout << "[Error] Syntax error for comparison operator, try using \'var != value\'\n";
                            return true;
                        }
                        cond.comp_code = 1;
                        cond.value_is_null = false;
                        
                        cond.value = extract_value(where.substr(2));
                        if(cond.value == ""){
                            return true;
                        }
                        break;
                    }
                    case '<':{
                        if(where[1] == '='){
                            cond.comp_code = 4;
                            cond.value_is_null = false;
                            
                            cond.value = extract_value(where.substr(2));
                            if(cond.value == ""){
                                return true;
                            }
                        }
                        else{
                            cond.comp_code = 2;
                            cond.value_is_null = false;
                            
                            cond.value = extract_value(where.substr(1));
                            if(cond.value == ""){
                                return true;
                            }
                        }
                        break;
                    }
                    case '>':{
                        if(where[1] == '='){
                            cond.comp_code = 5;
                            cond.value_is_null = false;
                            
                            cond.value = extract_value(where.substr(2));
                            if(cond.value == ""){
                                return true;
                            }
                        }
                        else{
                            cond.comp_code = 3;
                            cond.value_is_null = false;
                            
                            cond.value = extract_value(where.substr(1));
                            if(cond.value == ""){
                                return true;
                            }
                        }
                        break;
                    }
                    case ' ':{
                        cond.value_is_null = true;
                        cond.value = "null";
                        
                        stringstream where_ss(where);
                        string is_keyword = extract_word(where_ss);
                        string value_keyword = extract_word(where_ss);
                        if(value_keyword == "null"){
                            cond.comp_code = 6;
                        }
                        else if(value_keyword == "not"){
                            value_keyword = extract_word(where_ss);
                            if(value_keyword == "null"){
                                cond.comp_code = 7;
                            }
                            else{
                                cout << "[Error] Did you mean 'is not null'?\n";
                                return true;
                            }
                        }
                        else{
                            cout << "[Error] Did you mean 'is null'?\n";
                            return true;
                        }
                        break;
                    }
                    default:{
                        cout << where;
                        cout << "[Error] Comparison operator not recognized\n";
                        return true;
                    }
                }
            }
            
            engine.update_records(table_name, "-", "0", cond, true);
        }
        else{
            cout << "[Error] Invalid command\n";
        }
        return true;
    }
};

#endif /* parser_hpp */
