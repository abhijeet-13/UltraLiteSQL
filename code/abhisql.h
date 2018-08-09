#ifndef abhisql_h
#define abhisql_h
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <vector>
#include <unordered_map>
#include "file_utils.h"
#include "bplus_tree.h"
using namespace std;


// a column datatype
typedef class column_type{
public:
    string column_name;             // name of the columns
    string data_type;               // data type of values in this column
    bool not_null;                  // if only non-null values are allowed in this columns
    column_type(){
        column_name = string("");
        data_type = string("");
        not_null = false;
    }
} column_type;


// a condition datatype
class where_condition{
public:
    string column_name;             // the column whose values will be used for comparison
    uint8_t comp_code;              // code for the comparison operator
    string value;                   // the value in construct: column_name comp_code value
    bool value_is_null;             // true if value is to be interpreted as NULL
    
    // default condition is where (row_id > 0)
    where_condition(){
        column_name = "row_id";
        comp_code = 3;
        value = "0";
        value_is_null = false;
    }
    
    where_condition(string c, uint8_t o, string v){
        column_name = c;
        comp_code = o;
        value = v;
        value_is_null = false;
    }
    
};



// SQL Engine class
class Abhi_sql_engine{
    // tmp variables
    char tmp_page[PAGE_SIZE];
    string tmp_string;
    uint8_t tmp8;
    uint16_t tmp16;
    uint32_t tmp32;
    uint64_t tmp64;
    
    // text to code mapping for datatypes
    unordered_map<string, uint8_t> data_type_map;
    
public:
    Abhi_sql_engine(){
        data_type_map["tinyint"] = 0x04;
        data_type_map["smallint"] = 0x05;
        data_type_map["int"] = 0x06;
        data_type_map["bigint"] = 0x07;
        data_type_map["real"] = 0x08;
        data_type_map["double"] = 0x09;
        data_type_map["datetime"] = 0x0a;
        data_type_map["date"] = 0x0b;
        data_type_map["text"] = 0x0c;
    }
    

    // Show a list of all the saved tables (catalog + user_data)
    void show_tables(){
        cout << "Following tables in the database:\n";
        where_condition cond;
        vector<record_type> recs = get_all_records("database_tables");
        for(int i = 0; i < recs.size(); i++){
            
            string table_name = recs[i].second[0].second;
            
            /*
            fstream f;
            f.open(string("user_data/") + table_name + ".tbl", ios::in);
            if(!f.is_open()){
                f.open(string("catalog/") + table_name + ".tbl", ios::in);
                if(!f.is_open()){
                    continue;
                }
            }
            */
            
            cout << table_name << "\n";
        }
        cout << "\n";
    }
    
    
    // Drop a user-table from the database
    void drop_table(string table_name){
        cout << "Deleting user-table \'" << table_name << "\'\n";
        // 1. Delete record from database files
        // 2. Delete file from the user_data (cannot delete database file)
        
        where_condition cond1("table_name", 0, table_name);
        vector<record_type> recs = select_records(get_all_records("database_tables"), "database_tables", cond1);
        if(recs.size() == 0){
            cout << "No such table found\n";
            return;
        }
        where_condition cond2("table_name", 0, table_name);
        update_records("database_tables", "table_name", "-", cond2, true);
        
        where_condition cond3("table_name", 0, table_name);
        update_records("database_columns", "-", "-", cond3, true);

        string command;
        command = string("rm -f user_data/") + table_name + ".tbl";
        std::system(command.c_str());
        command = string("rm -f user_data/") + table_name + ".tbr";
        std::system(command.c_str());
        cout << "\n";
    }
    
    
    
    // Insert a full-qualified record in a table (catalog / user_data)
    bool insert(std::string table_name, record_type record, bool system_table = false){
        // find table path
        std::string table_file_path;
        if(!system_table){
            table_file_path = string("user_data/") + table_name + ".tbl";
        }
        else{
            table_file_path = string("catalog/") + table_name + ".tbl";
        }
        
        // check if a table exists with this name
        fstream f;
        f.open(table_file_path, ios::in);
        if(!f.is_open()){
            cout << "[Error] Cannot insert. No such table exists\n";
            return false;
        }
        f.close();
        
        // btree insert record in the table
        btree_utils::btree_insert(table_file_path, record);
        return true;
    }
    
    
    // Insert a half-qualified record in a user-table by converting to full-qualified record first
    bool insert(string table_name, vector<string> &insert_values, vector<string> &insert_columns){
        
        // obtain columns info about table
        where_condition cond("table_name", 0, table_name);
        vector<record_type> column_info = select_records(get_all_records("database_columns"), "database_columns", cond);

        
        // if insert_columns is empty, all values were input:
        if(insert_columns.size() == 0){
            // Ensure insert_values.size() == total_columns and first column is row_id
            if(insert_values.size() != column_info.size() + 1){
                cout << "[Error] Insufficient values for " << column_info.size() + 1 << " columns including row_id\n";
                return false;
            }
            
            // fill all the columns in insert_columns in ordinal order
            insert_columns.resize(column_info.size() + 1);
            insert_columns[0] = "row_id";
            for(int i = 0; i < column_info.size(); i++){
                int col_position, col_type;
                bool is_nullable;
                stringstream(column_info[i].second[3].second) >> col_position;
                stringstream(column_info[i].second[2].second) >> col_type;
                stringstream(column_info[i].second[4].second) >> is_nullable;
                insert_columns[col_position - 1 + 1] = column_info[i].second[1].second;
            }
        }

        
        // fill the hashmap
        unordered_map<string, string> col_values_map;
        for(int i = 0; i < insert_columns.size(); i++){
            if(col_values_map.find(insert_columns[i]) != col_values_map.end()){
                cout << "[Error] Value assigned to column \'" << insert_columns[i] << "\' multiple times\n";
                return false;
            }
            col_values_map[insert_columns[i]] = insert_values[i];
        }

        
        // prepare the record to be outputted
        record_type record;
        stringstream(col_values_map["row_id"]) >> record.first;
        
        for(int i = 0; i < column_info.size(); i++){
            int col_type;
            bool is_nullable;
            stringstream(column_info[i].second[2].second) >> col_type;
            stringstream(column_info[i].second[4].second) >> is_nullable;
            string col_name = column_info[i].second[1].second;
            
            if(col_values_map.find(col_name) != col_values_map.end() && col_values_map[col_name] != "null"){
                string col_value = col_values_map[col_name];
                if(col_type >= 0x0c){
                    col_type = 0x0c + (int) col_value.size();
                }
                record.second.push_back(make_pair(col_type, col_value));
            }
            else{
                if(!is_nullable){
                    cout << "[Error] Column \'" << col_name << "\' cannot be null\n";
                    return false;
                }
                else{
                    if(col_type == 0x07 || col_type == 0x09 || col_type == 0x0a || col_type == 0x0b)
                        col_type = 0x03;
                    if(col_type == 0x06 || col_type == 0x08)
                        col_type = 0x02;
                    if(col_type == 0x05)
                        col_type = 0x01;
                    if(col_type == 0x04)
                        col_type = 0x00;
                    record.second.push_back(make_pair(col_type, ""));
                }
            }
        }
        
        
        // ensure that the row_id value is strictly greater than the max_row_id so far
        // insert the record
        return insert(table_name, record);
    }
    
    
    // Create a user-table (catalog too, if required) with columns info provided
    bool create_table(string table_name, vector<column_type> &columns, bool system_table = false){
        // Check if table does not already exist
        fstream ftmp;
        string table_file_path = string("user_data/") + table_name + ".tbl";
        FILE *table_file = fopen(table_file_path.c_str(), "r");
        if(table_file){
            cout << "[Error] User table already exists\n";
            fclose(table_file);
            return false;
        }
        
        // sanity checks
        if(table_name == ""){
            cout << "[Error] Cannot read a valid table name\n";
            return false;
        }
        if(columns.size() == 0){
            cout << "[Warning] Are you sure, you want to create a table with no columns but row_id though?\n";
        }
        
        // Create table file
        uint8_t *first_page = file_utils::create_new_page(0x0d);
        file_utils::append_page_to_table_file(table_file_path, first_page);
        //file_utils::write_page_to_table_file(table_file_path, 0, first_page);
        
        // no need to update the catalog if it's a system table
        if(system_table)
            return true;
        
        
        // create root page file
        string table_root_path = string("user_data/") + table_name + ".tbr";
        fstream f;
        f.open(table_root_path, ios::out);
        f << 0;
        f.close();
        

        // read max_rowid in tables
        uint32_t tables_max_row_id;
        ftmp.open("catalog/database_tables_max_rowid.txt", ios::in);
        ftmp >> tables_max_row_id;
        ftmp.close();
        // insert entry in database_tables
        std::vector<std::pair<uint8_t, std::string> > record;
        record.push_back(make_pair(0x0c + table_name.size(), table_name));
        insert("database_tables", make_pair(tables_max_row_id + 1, record), true);
        // update max rowid
        ftmp.open("catalog/database_tables_max_rowid.txt", ios::out);
        ftmp << tables_max_row_id + 1;
        ftmp.close();
        
        // read max rowid in columns
        uint32_t columns_max_row_id;
        ftmp.open("catalog/database_columns_max_rowid.txt", ios::in);
        ftmp >> columns_max_row_id;
        ftmp.close();
        //insert a record for each column
        for(int i = 0; i < columns.size(); i++){
            if(columns[i].column_name == "" || data_type_map.find(columns[i].data_type) == data_type_map.end()){
                cout << "[Error] Invalid column information to create_tables() method\n";
                return false;
            }
            record.clear();
            record.push_back(make_pair(0x0c + table_name.size(), table_name));
            record.push_back(make_pair(0x0c + columns[i].column_name.size(), columns[i].column_name));
            record.push_back(make_pair(0x04, to_string((int) data_type_map[columns[i].data_type])));
            record.push_back(make_pair(0x04, to_string(i + 1)));
            record.push_back(make_pair(0x04, to_string((int) !columns[i].not_null)));
            insert("database_columns", make_pair(columns_max_row_id + 1 + i, record), true);
        }
        // update max row id
        ftmp.open("catalog/database_columns_max_rowid.txt", ios::out);
        ftmp << columns_max_row_id + columns.size();
        ftmp.close();
        
        return true;
    }
    
    
    vector<record_type> select_records(const vector<record_type> &all_records, string table_name, where_condition cond, vector<string> projection_columns = vector<string>()){
        
        // Obtain position info of condition column
        int ordinal_position = -1;
        int data_type = -1;
        vector<record_type> column_records = get_all_records("database_columns");
        
        if(cond.column_name != "row_id"){
            for(int i = 0; i < column_records.size(); i++){
                // check first and second column
                if(column_records[i].second[0].second == table_name && column_records[i].second[1].second == cond.column_name){
                    stringstream(column_records[i].second[2].second) >> data_type;
                    stringstream(column_records[i].second[3].second) >> ordinal_position;
                    break;
                }
            }
            if(ordinal_position == - 1){
                cout << "Column does not exist with name " << cond.column_name << " in table " << table_name << "\n";
                return vector<record_type>();
            }
            ordinal_position -= 1;      // Convert to 0 base
        }
        
        
        // Choose records with record at ordinal position satisfying the condition
        vector<record_type> selected_records;
        for(int i = 0; i < all_records.size(); i++){
            
            // add header unconditionally
            if(all_records[i].first == -1){
                selected_records.push_back(all_records[i]);
                continue;
            }
            
            if(cond.column_name == "row_id"){
                uint32_t rid;
                stringstream(cond.value) >> rid;
                bool select;
                switch(cond.comp_code){
                    case 0:
                        select = (all_records[i].first == rid);
                        break;
                    case 1:
                        select = (all_records[i].first != rid);
                        break;
                    case 2:
                        select = (all_records[i].first < rid);
                        break;
                    case 3:
                        select = (all_records[i].first > rid);
                        break;
                    case 4:
                        select = (all_records[i].first <= rid);
                        break;
                    case 5:
                        select = (all_records[i].first >= rid);
                        break;
                    default:
                        select = false;
                        break;
                }
                
                if(select){
                    selected_records.push_back(all_records[i]);
                }
            }
            else{
                if(check_cond(all_records[i].second[ordinal_position], cond)){
                    selected_records.push_back(all_records[i]);
                }
            }
        }
        
        
        
        // if no specific projection columns were asked
        if(projection_columns.size() == 0)
            return selected_records;

        
        // Obtain ordinal positions for the projection columns
        vector<int> projection_ordinal_positions;
        for(int j = 0; j < projection_columns.size(); j++){
            if(projection_columns[j] == "*"){
                return selected_records;
            }
            ordinal_position = -1;
            data_type = -1;
            for(int i = 0; i < column_records.size(); i++){
                if(column_records[i].second[0].second == table_name && column_records[i].second[1].second == projection_columns[j]){
                    stringstream(column_records[i].second[3].second) >> ordinal_position;
                    break;
                }
            }
            if(ordinal_position == - 1){
                cout << "[Warning] Cannot find projection column \'" << projection_columns[j] << "\'\n";
                continue;
            }
            projection_ordinal_positions.push_back(ordinal_position - 1);
        }
        
        // project the columns and output
        vector<record_type> output_records;
        for(int i = 0; i < selected_records.size(); i++){
            record_type r;
            r.first = selected_records[i].first;
            for(int j = 0; j < projection_ordinal_positions.size(); j++){
                r.second.push_back(selected_records[i].second[projection_ordinal_positions[j]]);
            }
            output_records.push_back(r);
        }
        return output_records;
        
    }
    
    
    
    bool check_cond(pair<uint8_t, string> type_value_pair, where_condition cond){
        
        switch(type_value_pair.first){
            // null values
            case 0x00:
            case 0x01:
            case 0x02:
            case 0x03:{
                if(!cond.value_is_null){
                    return ((cond.comp_code == 1) || (cond.comp_code == 7));
                }
                return (cond.comp_code == 6);
            }
            
            // non null values
            case 0x04:
            case 0x05:
            case 0x06:
            case 0x07:{
                if(cond.value_is_null){
                    return ((cond.comp_code == 1) || (cond.comp_code == 7));
                }
                int64_t col_value, comp_value;
                stringstream(type_value_pair.second) >> col_value;
                
                stringstream(cond.value) >> comp_value;
                switch(cond.comp_code){
                    case 0:
                        return (col_value == comp_value);
                    case 1:
                        return (col_value != comp_value);
                    case 2:
                        return (col_value < comp_value);
                    case 3:
                        return (col_value > comp_value);
                    case 4:
                        return (col_value <= comp_value);
                    case 5:
                        return (col_value >= comp_value);
                    case 6:
                        return (col_value == comp_value);
                    case 7:
                        return (col_value != comp_value);
                    default:{
                        cout << "[Warning] Unknown comparison code\n";
                        return false;
                    }
                }
            }
            case 0x08:{
                if(cond.value_is_null){
                    return ((cond.comp_code == 1) || (cond.comp_code == 7));
                }
                float col_value, comp_value;
                stringstream(type_value_pair.second) >> col_value;
                stringstream(cond.value) >> comp_value;
                switch(cond.comp_code){
                    case 0:
                        return (col_value == comp_value);
                    case 1:
                        return (col_value != comp_value);
                    case 2:
                        return (col_value < comp_value);
                    case 3:
                        return (col_value > comp_value);
                    case 4:
                        return (col_value <= comp_value);
                    case 5:
                        return (col_value >= comp_value);
                    case 6:
                        return (col_value == comp_value);
                    case 7:
                        return (col_value != comp_value);
                    default:{
                        cout << "[Warning] Unknown comparison code\n";
                        return false;
                    }
                }
            }
            case 0x09:{
                if(cond.value_is_null){
                    return ((cond.comp_code == 1) || (cond.comp_code == 7));
                }
                double col_value, comp_value;
                stringstream(type_value_pair.second) >> col_value;
                stringstream(cond.value) >> comp_value;
                switch(cond.comp_code){
                    case 0:
                        return (col_value == comp_value);
                    case 1:
                        return (col_value != comp_value);
                    case 2:
                        return (col_value < comp_value);
                    case 3:
                        return (col_value > comp_value);
                    case 4:
                        return (col_value <= comp_value);
                    case 5:
                        return (col_value >= comp_value);
                    case 6:
                        return (col_value == comp_value);
                    case 7:
                        return (col_value != comp_value);
                    default:{
                        cout << "[Warning] Unknown comparison code\n";
                        return false;
                    }
                }
            }
            case 0x0a:{
                if(cond.value_is_null){
                    return ((cond.comp_code == 1) || (cond.comp_code == 7));
                }
                uint64_t col_value, comp_value;
                std::string data_string = type_value_pair.second;
                struct tm ttm = {0};
                if(data_string.size() == 19){
                    std::string s_year = data_string.substr(0, 4);
                    std::string s_month = data_string.substr(5, 2);
                    std::string s_date = data_string.substr(8, 2);
                    std::string s_hh = data_string.substr(11, 2);
                    std::string s_mm = data_string.substr(14, 2);
                    std::string s_ss = data_string.substr(17, 2);
                    if(s_year.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_year = stoi(s_year) - 1900;
                    if(s_month.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_mon = stoi(s_month) - 1;
                    if(s_date.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_mday = stoi(s_date);
                    if(s_hh.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_hour = stoi(s_hh) - 1;
                    if(s_mm.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_min = stoi(s_mm);
                    if(s_ss.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_sec = stoi(s_ss);
                }
                col_value = mktime(&ttm);
                
                data_string = cond.value;
                ttm = (const struct tm){0};
                if(data_string.size() == 19){
                    std::string s_year = data_string.substr(0, 4);
                    std::string s_month = data_string.substr(5, 2);
                    std::string s_date = data_string.substr(8, 2);
                    std::string s_hh = data_string.substr(11, 2);
                    std::string s_mm = data_string.substr(14, 2);
                    std::string s_ss = data_string.substr(17, 2);
                    if(s_year.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_year = stoi(s_year) - 1900;
                    if(s_month.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_mon = stoi(s_month) - 1;
                    if(s_date.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_mday = stoi(s_date);
                    if(s_hh.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_hour = stoi(s_hh) - 1;
                    if(s_mm.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_min = stoi(s_mm);
                    if(s_ss.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_sec = stoi(s_ss);
                }
                comp_value = mktime(&ttm);
                
                switch(cond.comp_code){
                    case 0:
                        return difftime(col_value, comp_value) == 0;
                    case 1:
                        return difftime(col_value, comp_value) != 0;
                    case 2:
                        return difftime(col_value, comp_value) < 0;
                    case 3:
                        return difftime(col_value, comp_value) > 0;
                    case 4:
                        return difftime(col_value, comp_value) <= 0;
                    case 5:
                        return difftime(col_value, comp_value) >= 0;
                    case 6:
                        return difftime(col_value, comp_value) == 0;
                    case 7:
                        return difftime(col_value, comp_value) != 0;
                    default:{
                        cout << "[Warning] Unknown comparison code\n";
                        return false;
                    }
                }
            }
            {
                if(cond.value_is_null){
                    return ((cond.comp_code == 1) || (cond.comp_code == 7));
                }
                uint64_t col_value, comp_value;
                stringstream(type_value_pair.second) >> col_value;
                
                std::string data_string = cond.value;
                struct tm ttm = {0};
                if(data_string.size() == 19){
                    std::string s_year = data_string.substr(0, 4);
                    std::string s_month = data_string.substr(5, 2);
                    std::string s_date = data_string.substr(8, 2);
                    std::string s_hh = data_string.substr(11, 2);
                    std::string s_mm = data_string.substr(14, 2);
                    std::string s_ss = data_string.substr(17, 2);
                    if(s_year.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_year = stoi(s_year) - 1900;
                    if(s_month.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_mon = stoi(s_month) - 1;
                    if(s_date.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_mday = stoi(s_date);
                    if(s_hh.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_hour = stoi(s_hh) - 1;
                    if(s_mm.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_min = stoi(s_mm);
                    if(s_ss.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_sec = stoi(s_ss);
                }
                comp_value = mktime(&ttm);
                
                switch(cond.comp_code){
                    case 0:
                        return difftime(col_value, comp_value) == 0;
                    case 1:
                        return difftime(col_value, comp_value) != 0;
                    case 2:
                        return difftime(col_value, comp_value) < 0;
                    case 3:
                        return difftime(col_value, comp_value) > 0;
                    case 4:
                        return difftime(col_value, comp_value) <= 0;
                    case 5:
                        return difftime(col_value, comp_value) >= 0;
                    case 6:
                        return difftime(col_value, comp_value) == 0;
                    case 7:
                        return difftime(col_value, comp_value) != 0;
                    default:{
                        cout << "[Warning] Unknown comparison code\n";
                        return false;
                    }
                }
            }
            case 0x0b:{
                if(cond.value_is_null){
                    return ((cond.comp_code == 1) || (cond.comp_code == 7));
                }
                uint64_t col_value, comp_value;
                std::string data_string = type_value_pair.second;
                struct tm ttm = {0};
                if(data_string.size() == 10){
                    std::string s_year = data_string.substr(0, 4);
                    std::string s_month = data_string.substr(5, 2);
                    std::string s_date = data_string.substr(8, 2);
                    if(s_year.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_year = stoi(s_year) - 1900;
                    if(s_month.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_mon = stoi(s_month) - 1;
                    if(s_date.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_mday = stoi(s_date);
                }
                col_value = mktime(&ttm);
                
                data_string = cond.value;
                ttm = (const struct tm){0};
                if(data_string.size() == 10){
                    std::string s_year = data_string.substr(0, 4);
                    std::string s_month = data_string.substr(5, 2);
                    std::string s_date = data_string.substr(8, 2);
                    if(s_year.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_year = stoi(s_year) - 1900;
                    if(s_month.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_mon = stoi(s_month) - 1;
                    if(s_date.find_first_not_of("0123456789") == std::string::npos)
                        ttm.tm_mday = stoi(s_date);
                }
                comp_value = mktime(&ttm);
                
                switch(cond.comp_code){
                    case 0:
                        return difftime(col_value, comp_value) == 0;
                    case 1:
                        return difftime(col_value, comp_value) != 0;
                    case 2:
                        return difftime(col_value, comp_value) < 0;
                    case 3:
                        return difftime(col_value, comp_value) > 0;
                    case 4:
                        return difftime(col_value, comp_value) <= 0;
                    case 5:
                        return difftime(col_value, comp_value) >= 0;
                    case 6:
                        return difftime(col_value, comp_value) == 0;
                    case 7:
                        return difftime(col_value, comp_value) != 0;
                    default:{
                        cout << "[Warning] Unknown comparison code\n";
                        return false;
                    }
                }
            }
            default:{
                if(cond.value_is_null){
                    return ((cond.comp_code == 6 && type_value_pair.first == 0x0c) ||
                            (cond.comp_code == 7 && type_value_pair.first > 0x0c));
                }
                switch(cond.comp_code){
                    case 0:
                        return (type_value_pair.second == cond.value);
                    case 1:
                        return (type_value_pair.second != cond.value);
                    case 2:
                        return (type_value_pair.second < cond.value);
                    case 3:
                        return (type_value_pair.second > cond.value);
                    case 4:
                        return (type_value_pair.second <= cond.value);
                    case 5:
                        return (type_value_pair.second >= cond.value);
                    default:
                        return false;
                }
            }
        }
    }
    
    
    // Obtain all the fully qualified records from a table
    vector<record_type> get_all_records(string table_name, bool add_header_at_top = false){
        
        // Ensure a table entry exists for table_name
        // Ensure a table_entry and column_entry pair exists for each column_name
        
        vector<record_type> all_records;
        
        if(add_header_at_top){
            record_type header;
            header.first = -1;
            
            where_condition cond;
            cond.column_name = "table_name";
            cond.comp_code = 0;
            cond.value = table_name;
            vector<record_type> column_info = select_records(get_all_records("database_columns"), "database_columns", cond);
            for(int i = 0; i < column_info.size(); i++){
                string col_name = column_info[i].second[1].second;
                header.second.push_back(make_pair(0x0c + col_name.size(), col_name));
            }
            all_records.push_back(header);
        }
        
        // Look for the table file
        string table_file_path, table_root_path;
        fstream table_file;
        table_file_path = string("user_data/") + table_name + ".tbl";
        table_root_path = string("user_data/") + table_name + ".tbr";
        table_file.open(table_file_path, ios::in);
        if(!table_file.is_open()){
            table_file_path = string("catalog/") + table_name + ".tbl";
            table_root_path = string("catalog/") + table_name + ".tbr";
            table_file.open(table_file_path, ios::in);
        }
        if(!table_file.is_open()){
            cout << "[Error] No such table exists\n";
            return all_records;
        }
        
        uint32_t root_page_addr;
        fstream f;
        f.open(table_root_path, ios::in);
        f >> root_page_addr;
        f.close();
        
        
        // Obtain first page from the table
        uint8_t leaf_page[PAGE_SIZE];
        file_utils::read_page_from_table_file(table_file_path, root_page_addr / PAGE_SIZE, leaf_page);
        while(leaf_page[0] != 0x0d){
            string value_string;
            file_utils::read_value_within_page(leaf_page, PAGE_SIZE - 8, value_string, 0x06);
            uint32_t left_most_child_addr = (uint32_t) stol(value_string);
            file_utils::read_page_from_table_file(table_file_path, left_most_child_addr / PAGE_SIZE, leaf_page);
        }
        
        
        // traverse to a leaf page, if root is not leaf
        if(leaf_page[0] != 0x0d){
            cout << "Root page is not a leaf page\n";
            return all_records;
        }

        
        // Scan through all the pages in the table
        while(leaf_page[0] == 0x0d){
            
            // total records in current page
            uint8_t records_in_page;
            file_utils::page_read(leaf_page, 1, records_in_page);
            
            // for each record
            for(int i = 0; i < records_in_page; i++){
                // obtain offset within the page of that record
                uint16_t addr;
                file_utils::page_read(leaf_page, 8 + 2 * i, addr);
                
                // obtain 4 byte row_id
                uint32_t row_id;
                file_utils::page_read(leaf_page, addr + 2, row_id);
                
                // obtain number of columns in the record
                uint8_t tot_columns;
                file_utils::page_read(leaf_page, addr + 6, tot_columns);

                // obtain type_codes of all the columns
                vector<uint8_t> type_codes;
                for(int j = 0; j < tot_columns; j++){
                    uint8_t type_code;
                    file_utils::page_read(leaf_page, addr + 7 + j, type_code);
                    type_codes.push_back(type_code);
                }
                
                // Read values for all the columns
                vector<pair<uint8_t, string> > record_columns;
                size_t offset = addr + 7 + tot_columns;
                for(int j = 0; j < tot_columns; j++){
                    string data_string;
                    offset = file_utils::read_value_within_page(leaf_page, offset, data_string, type_codes[j]);
                    record_columns.push_back(make_pair(type_codes[j], data_string));
                }
                
                // add the read record to the list of records
                all_records.push_back(make_pair(row_id, record_columns));
            }

            
            string value_string;
            file_utils::read_value_within_page(leaf_page, 4, value_string, 0x06);
            uint32_t right_page_addr = (uint32_t) stol(value_string);
            if(right_page_addr == 0xffffffff)
                break;
            file_utils::read_page_from_table_file(table_file_path, right_page_addr / PAGE_SIZE, leaf_page);
        }
        
        return all_records;
    }
    
    
    
    
    
    
    
    // Display records to the console
    void display_records(const vector<record_type> &records){
        if(records.size() == 0){
            cout << "0 records to display\n\n";
            return;
        }
        if(records.size() == 1 && records[0].first == -1){
            cout << "0 records to display\n\n";
            return;
        }
        cout << '\n';
        vector<size_t> col_widths(records[0].second.size(), 0);
        size_t width = 5;
        for(int j = 0; j < col_widths.size(); j++){
            for(int i = 0; i < records.size(); i++){
                if(records[i].second[j].second.size() > col_widths[j]){
                    col_widths[j] = records[i].second[j].second.size();
                }
            }
            width += (col_widths[j] + 3);
        }

        cout << ' ';
        cout << string(width, '-');
        cout << '\n';
        for(int i = 0; i < records.size(); i++){
            cout << '|';
            if(records[i].first == -1)
                cout << tabulate_entry("id", 5) << '|';
            else
                cout << tabulate_entry(to_string(records[i].first), 5) << '|';
            vector<pair<uint8_t, string> > rc = records[i].second;
            for(int j = 0; j < rc.size(); j++){
                cout << tabulate_entry(rc[j].second, col_widths[j] + 2) << '|';
            }
            cout << '\n';
            
            if(records[i].first == -1){
                cout << '|';
                cout << string(width, '-');
                cout << "|\n";
            }
        }
        cout << ' ';
        cout << string(width, '-');
        cout << '\n' << records.size() - ((records[0].first == -1) ? 1 : 0) << " records returned.\n";
    }
    
    
    string tabulate_entry(string entry, size_t width){
        if(entry.size() >= width){
            return entry;
        }
        size_t before = 1;
        return string(before, ' ') + entry + string(width - before - entry.size(), ' ');
    }
    
    
    bool update_records(string table_name, string column_name, string value, where_condition cond, bool delete_record = false){
        
        // find ordinal positions of both columns
        where_condition col_cond("table_name", 0, table_name);
        vector<record_type> column_info = select_records(get_all_records("database_columns"), "database_columns", col_cond);
        int pos_c = -1;
        int pos_u = -1;
        uint8_t new_dt = -1;
        for(int i = 0; i < column_info.size(); i++){
            int col_position = stoi(column_info[i].second[3].second);
            string col_name = column_info[i].second[1].second;
            
            if(col_name == column_name){
                pos_u = col_position - 1;
                new_dt = stoi(column_info[i].second[2].second);
            }
            if(col_name == cond.column_name){
                pos_c = col_position - 1;
            }
        }
        if(cond.column_name == "row_id"){
            pos_c = -2;
        }
        if(column_name == "row_id"){
            pos_u = -2;
            cout << "[Error] Updating the row_id is not allowed\n";
            return false;
        }
        
        if(pos_c == -1){
            cout << "[Error] Invalid column name in condition\n";
            return false;
        }
        if(pos_u == -1 && !delete_record){
            cout << "[Error] No column named \'" << column_name << "\' in table \'" << table_name << "\'\n";
            return false;
        }
        
        
        // Look for the table file
        string table_file_path, table_root_path;
        fstream table_file;
        table_file_path = string("user_data/") + table_name + ".tbl";
        table_root_path = string("user_data/") + table_name + ".tbr";
        table_file.open(table_file_path, ios::in);
        if(!table_file.is_open()){
            table_file_path = string("catalog/") + table_name + ".tbl";
            table_root_path = string("catalog/") + table_name + ".tbr";
            table_file.open(table_file_path, ios::in);
        }
        if(!table_file.is_open()){
            cout << "[Error] No such table exists\n";
            return false;
        }
        
        uint32_t root_page_addr;
        fstream f;
        f.open(table_root_path, ios::in);
        f >> root_page_addr;
        f.close();
        
        
        // Obtain first page from the table
        uint8_t leaf_page[PAGE_SIZE];
        file_utils::read_page_from_table_file(table_file_path, root_page_addr / PAGE_SIZE, leaf_page);
        uint32_t leaf_addr = root_page_addr;
        while(leaf_page[0] != 0x0d){
            string value_string;
            file_utils::read_value_within_page(leaf_page, PAGE_SIZE - 8, value_string, 0x06);
            leaf_addr = (uint32_t) stol(value_string);
            file_utils::read_page_from_table_file(table_file_path, leaf_addr / PAGE_SIZE, leaf_page);
        }
        
        
        // Scan through all the pages in the table
        while(leaf_page[0] == 0x0d){
            
            if(delete_record){
                bool item_deleted = true;
                while(item_deleted){
                    item_deleted = false;
                    
                    uint8_t records_in_page;
                    file_utils::page_read(leaf_page, 1, records_in_page);
                    for(int i = 0; i < records_in_page; i++){
                        uint16_t addr;
                        file_utils::page_read(leaf_page, 8 + 2 * i, addr);
                        uint32_t row_id;
                        file_utils::page_read(leaf_page, addr + 2, row_id);
                        uint8_t tot_columns;
                        file_utils::page_read(leaf_page, addr + 6, tot_columns);
                        vector<uint8_t> type_codes;
                        for(int j = 0; j < tot_columns; j++){
                            uint8_t type_code;
                            file_utils::page_read(leaf_page, addr + 7 + j, type_code);
                            type_codes.push_back(type_code);
                        }
                        vector<pair<uint8_t, string> > record_columns;
                        size_t offset = addr + 7 + tot_columns;
                        for(int j = 0; j < tot_columns; j++){
                            string data_string;
                            offset = file_utils::read_value_within_page(leaf_page, offset, data_string, type_codes[j]);
                            record_columns.push_back(make_pair(type_codes[j], data_string));
                        }
                        bool record_found = false;
                        if(pos_c == -2){
                            record_found = check_cond(make_pair(0x06, to_string(row_id)), cond);
                        }
                        else{
                            record_found = check_cond(record_columns[pos_c], cond);
                        }
                        if(record_found){
                            file_utils::delete_record_from_page(leaf_page, row_id);
                            item_deleted = true;
                            break;
                        }
                    }
                }
                
            }
            else{
                // total records in current page
                uint8_t records_in_page;
                file_utils::page_read(leaf_page, 1, records_in_page);
                
                // for each record
                for(int i = 0; i < records_in_page; i++){
                    // obtain offset within the page of that record
                    uint16_t addr;
                    file_utils::page_read(leaf_page, 8 + 2 * i, addr);
                    
                    // obtain 4 byte row_id
                    uint32_t row_id;
                    file_utils::page_read(leaf_page, addr + 2, row_id);
                    
                    
                    // obtain number of columns in the record
                    uint8_t tot_columns;
                    file_utils::page_read(leaf_page, addr + 6, tot_columns);
                    
                    // obtain type_codes of all the columns
                    vector<uint8_t> type_codes;
                    for(int j = 0; j < tot_columns; j++){
                        uint8_t type_code;
                        file_utils::page_read(leaf_page, addr + 7 + j, type_code);
                        type_codes.push_back(type_code);
                    }
                    
                    // Read values for all the columns
                    vector<pair<uint8_t, string> > record_columns;
                    size_t offset = addr + 7 + tot_columns;
                    
                    for(int j = 0; j < tot_columns; j++){
                        string data_string;
                        offset = file_utils::read_value_within_page(leaf_page, offset, data_string, type_codes[j]);
                        record_columns.push_back(make_pair(type_codes[j], data_string));
                    }
                    bool record_found = false;
                    if(pos_c == -2){
                        record_found = check_cond(make_pair(0x06, to_string(row_id)), cond);
                    }
                    else{
                        record_found = check_cond(record_columns[pos_c], cond);
                    }
                    
                    if(value == "null"){
                        if(new_dt == 0x07 || new_dt == 0x09 || new_dt == 0x0a || new_dt == 0x0b)
                            new_dt = 0x03;
                        if(new_dt == 0x06 || new_dt == 0x08)
                            new_dt = 0x02;
                        if(new_dt == 0x05)
                            new_dt = 0x01;
                        if(new_dt == 0x04)
                            new_dt = 0x00;
                    }
                    
                    if(record_found){
                        offset = addr + 7 + tot_columns;
                        for(int j = 0; j < tot_columns; j++){
                            string data_string;
                            if(j == pos_u){
                                if(type_codes[j] > 0x0c){
                                    new_dt = type_codes[j];
                                    if(value.size() < new_dt - 0x0c){
                                        value = value + string(new_dt - 0x0c - value.size(), ' ');
                                    }
                                }
                                
                                file_utils::add_value_within_page(leaf_page, addr + 7 + j, to_string(new_dt), 0x04);
                                offset = file_utils::add_value_within_page(leaf_page, offset, value, new_dt);
                            }
                            else{
                                offset = file_utils::read_value_within_page(leaf_page, offset, data_string, type_codes[j]);
                                record_columns.push_back(make_pair(type_codes[j], data_string));
                            }
                        }
                    }
                }
            }

            // update the page
            
            file_utils::write_page_to_table_file(table_file_path, leaf_addr / PAGE_SIZE, leaf_page);
            string value_string;
            file_utils::read_value_within_page(leaf_page, 4, value_string, 0x06);
            leaf_addr = (uint32_t) stol(value_string);
            if(leaf_addr == 0xffffffff)
                break;
            file_utils::read_page_from_table_file(table_file_path, leaf_addr / PAGE_SIZE, leaf_page);
        }
        
        return true;
    }
    
    
    
    
    // install / reset the engine
    void install();
    

};





void Abhi_sql_engine::install(){
    cout << "Installing and resetting..\n";
    
    // remove existing
    system("rm -rf catalog");
    system("rm -rf user_data");
    
    // Create directories
    system("mkdir catalog");
    system("mkdir user_data");
    uint8_t first_page[PAGE_SIZE];
    size_t offset = 0;
    
    
    // Prepare the first page, and write to the file
    memset(first_page, 0, PAGE_SIZE);
    memset(first_page + offset++, 0x0d, 1);
    memset(first_page + offset++, 0x00, 1);
    memcpy(first_page + offset, file_utils::byte_pattern(512, 2), 2);
    offset += 2;
    memcpy(first_page + offset, file_utils::byte_pattern(0xffffffff, 4), 4);
    offset += 4;
    
    string table_file_path("catalog/database_tables.tbl");
    string column_file_path("catalog/database_columns.tbl");
    
    file_utils::append_page_to_table_file(table_file_path, first_page);
    file_utils::append_page_to_table_file(column_file_path, first_page);
    
    
    // Add entries to the table
    vector<pair<uint32_t, vector<pair<uint8_t, string> > > > records;
    
    // tables file
    records.push_back(make_pair(0x0001, vector<pair<uint8_t, string> >()));
    records[0].second.push_back(make_pair(0x0c + 15, "database_tables"));
    records.push_back(make_pair(0x0002, vector<pair<uint8_t, string> >()));
    records[1].second.push_back(make_pair(0x0c + 16, "database_columns"));
    
    file_utils::read_page_from_table_file(table_file_path, 0, first_page);
    if(first_page[0] == 0x0d){
        for(int i = 0; i < records.size(); i++){
            file_utils::add_record_to_page(first_page, records[i]);
        }
    }
    file_utils::write_page_to_table_file(table_file_path, 0, first_page);
    
    // columns file
    records.clear();
    records.push_back(make_pair(0x0001, vector<pair<uint8_t, string> >()));
    records[0].second.push_back(make_pair(0x0c + 15, "database_tables"));
    records[0].second.push_back(make_pair(0x0c + 10, "table_name"));
    records[0].second.push_back(make_pair(0x04, "12"));
    records[0].second.push_back(make_pair(0x04, "1"));
    records[0].second.push_back(make_pair(0x04, "0"));
    records.push_back(make_pair(0x0002, vector<pair<uint8_t, string> >()));
    records[1].second.push_back(make_pair(0x0c + 16, "database_columns"));
    records[1].second.push_back(make_pair(0x0c + 10, "table_name"));
    records[1].second.push_back(make_pair(0x04, "12"));
    records[1].second.push_back(make_pair(0x04, "1"));
    records[1].second.push_back(make_pair(0x04, "0"));
    records.push_back(make_pair(0x0003, vector<pair<uint8_t, string> >()));
    records[2].second.push_back(make_pair(0x0c + 16, "database_columns"));
    records[2].second.push_back(make_pair(0x0c + 11, "column_name"));
    records[2].second.push_back(make_pair(0x04, "12"));
    records[2].second.push_back(make_pair(0x04, "2"));
    records[2].second.push_back(make_pair(0x04, "0"));
    records.push_back(make_pair(0x0004, vector<pair<uint8_t, string> >()));
    records[3].second.push_back(make_pair(0x0c + 16, "database_columns"));
    records[3].second.push_back(make_pair(0x0c + 9, "data_type"));
    records[3].second.push_back(make_pair(0x04, "4"));
    records[3].second.push_back(make_pair(0x04, "3"));
    records[3].second.push_back(make_pair(0x04, "0"));
    records.push_back(make_pair(0x0005, vector<pair<uint8_t, string> >()));
    records[4].second.push_back(make_pair(0x0c + 16, "database_columns"));
    records[4].second.push_back(make_pair(0x0c + 8, "position"));
    records[4].second.push_back(make_pair(0x04, "4"));
    records[4].second.push_back(make_pair(0x04, "4"));
    records[4].second.push_back(make_pair(0x04, "0"));
    records.push_back(make_pair(0x0006, vector<pair<uint8_t, string> >()));
    records[5].second.push_back(make_pair(0x0c + 16, "database_columns"));
    records[5].second.push_back(make_pair(0x0c + 11, "is_nullable"));
    records[5].second.push_back(make_pair(0x04, "4"));
    records[5].second.push_back(make_pair(0x04, "5"));
    records[5].second.push_back(make_pair(0x04, "0"));
    
    file_utils::read_page_from_table_file(column_file_path, 0, first_page);
    if(first_page[0] == 0x0d){
        for(int i = 0; i < records.size(); i++){
            file_utils::add_record_to_page(first_page, records[i]);
        }
    }
    file_utils::write_page_to_table_file(column_file_path, 0, first_page);
    
    // Add max row id files
    int value = 2;
    fstream ftmp;
    ftmp.open("catalog/database_tables_max_rowid.txt", ios::out);
    ftmp << value;
    ftmp.close();
    value = 6;
    ftmp.open("catalog/database_columns_max_rowid.txt", ios::out);
    ftmp << value;
    ftmp.close();
    
    
    ftmp.open("catalog/database_columns.tbr", ios::out);
    ftmp << 0;
    ftmp.close();
    
    ftmp.open("catalog/database_tables.tbr", ios::out);
    ftmp << 0;
    ftmp.close();
    
    cout << "Installed\n";
}


#endif /* abhisql_h */
