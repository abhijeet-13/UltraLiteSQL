#ifndef file_utils_h
#define file_utils_h
#include <iostream>
#include <vector>
#include <sstream>
#include <string>
#include <time.h>

#define PAGE_SIZE 512

typedef std::pair<uint32_t, std::vector<std::pair<uint8_t, std::string> > > record_type;

class file_utils{
    
public:
    
    // is big endian
    static bool is_big_endian();
    
    // find size of a record
    static size_t size_of_record(record_type &r);
    
    // find size of type code
    static uint16_t size_of_type_code(uint8_t type_code);
    
    // big-endian byte pattern of a multi byte value
    template <typename T>
    static const uint8_t* byte_pattern(T value, size_t total_bytes);
    
    
    // create, initialize and return a new page
    static uint8_t* create_new_page(uint8_t btree_node_type = 0x0d){
        uint8_t* new_page = new uint8_t[PAGE_SIZE];
        memset(new_page, 0, PAGE_SIZE);
        new_page[0] = btree_node_type;
        memcpy(new_page + 2, file_utils::byte_pattern(512, 2), 2);
        memcpy(new_page + 4, file_utils::byte_pattern(0xffffffff, 4), 4);
        return new_page;
    }
    
    
    // returns the page address of the newly added page
    static uint32_t append_page_to_table_file(std::string table_file_path, uint8_t btree_node_type = 0x0d){
        // obtain a pointer to a freshly created page
        uint8_t* new_page = create_new_page(btree_node_type);
        
        // seek to the end of the existing table file
        FILE* table_file = fopen(table_file_path.c_str(), "a");
        
        // append the new page to the end of this file
        uint32_t new_page_addr = (uint32_t) ftell(table_file);
        fwrite(new_page, sizeof(uint8_t), PAGE_SIZE, table_file);
        fclose(table_file);
        return new_page_addr;
    }
    
    
    // read an entire page from the file
    static void read_page_from_table_file(std::string table_file_path, int page_number, uint8_t *page);
    
    // write an entire page to the file
    static void write_page_to_table_file(std::string table_file_path, int page_number, const uint8_t *page);
    
    // append an entire page to the file
    static void append_page_to_table_file(std::string table_file_path, const uint8_t *page);

    
    
    // utility for reading a value from a page at some offset
    template<typename T>
    static size_t page_read(uint8_t* page_base, size_t offset, T &value);
    
    // add a record to a page
    static int add_record_to_page(uint8_t *table_leaf_page, std::pair<uint32_t, std::vector<std::pair<uint8_t, std::string> > > record);
    
    
    
    // remove a record from a page
    static bool delete_record_from_page(uint8_t* page, uint32_t delete_row_id){
        
        
        uint8_t number_of_existing_records;
        page_read(page, 1, number_of_existing_records);
        uint16_t content_area_boundary;
        page_read(page, 2, content_area_boundary);
        
        bool record_found = false;
        
        // check if key already exists
        std::vector<std::pair<uint32_t, uint16_t> > key_loc_pairs;
        for(int i = 0; i < number_of_existing_records; i++){
            uint16_t loc;
            uint32_t key;
            page_read(page, 8 + 2 * i, loc);
            page_read(page, loc + 2, key);
            if(key == delete_row_id){
                record_found = true;
            }
            else{
                key_loc_pairs.push_back(std::make_pair(key, loc));
            }
        }
        
        // return false if record not found in the page
        if(!record_found)
            return false;
        
        page[1]--;
        key_loc_pairs.push_back(std::make_pair(0, 0));
        for(int i = 0; i < key_loc_pairs.size(); i++){
            memcpy(page + 8 + 2 * i, file_utils::byte_pattern(key_loc_pairs[i].second, 2), 2);
        }
        return true;
    }
    
    
    // add a record's value within page
    static size_t add_value_within_page(uint8_t *page, size_t offset, std::string data_string, uint8_t type_code);
    
    // read a value from within the page
    static size_t read_value_within_page(uint8_t *page, size_t offset, std::string& data_string, uint8_t type_code);
    
    
    static bool comp_key_locs(std::pair<uint32_t, uint16_t> &v1, std::pair<uint32_t, uint16_t> &v2){
        return (v1.first < v2.first);
    }
    
};



template <typename T>
const uint8_t* file_utils::byte_pattern(T value, size_t total_bytes){
    std::unique_ptr<uint8_t> bytes_arr (new uint8_t[total_bytes]);
    *((T *) bytes_arr.get()) = value;
    if(!is_big_endian()){
        std::reverse(bytes_arr.get(), bytes_arr.get() + total_bytes);
    }
    return bytes_arr.get();
}


template<typename T>
size_t file_utils::page_read(uint8_t* page_base, size_t offset, T &value){
    uint8_t bytes_arr[8] = {0};
    memcpy(bytes_arr, page_base + offset, sizeof(T));
    if(!is_big_endian()){
        std::reverse(bytes_arr, bytes_arr + sizeof(T));
    }
    value = *((T *) bytes_arr);
    return offset + sizeof(T);
}



















// find endianness
bool file_utils::is_big_endian(){
    int endianness = 1;
    uint16_t check_value = 0x0001;
    uint8_t *check_value_bytes = (uint8_t*) &check_value;
    if(check_value_bytes[0] == 0x01)
        endianness = 0;
    return (endianness == 1);
}


// find size of a record
size_t file_utils::size_of_record(record_type &r){
    size_t record_size = 6;
    record_size += r.second.size();
    for(size_t i = 0; i < r.second.size(); i++){
        record_size += file_utils::size_of_type_code(r.second[i].first);
    }
    return record_size;
}


// find size of type code
uint16_t file_utils::size_of_type_code(uint8_t type_code){
    switch(type_code){
        case 0x00:
            return 1;
        case 0x01:
            return 2;
        case 0x02:
            return 4;
        case 0x03:
            return 8;
        case 0x04:
            return 1;
        case 0x05:
            return 2;
        case 0x06:
            return 4;
        case 0x07:
            return 8;
        case 0x08:
            return 4;
        case 0x09:
            return 8;
        case 0x0a:
            return 8;
        case 0x0b:
            return 8;
        default:
            return type_code - 0x0c;
    }
}


// Read an entire page from the table file, given the page number
void file_utils::read_page_from_table_file(std::string table_file_path, int page_number, uint8_t *page){
    FILE *table_file = fopen(table_file_path.c_str(), "r");
    fseek(table_file, page_number * PAGE_SIZE, SEEK_SET);
    fread(page, sizeof(uint8_t), PAGE_SIZE, table_file);
    fclose(table_file);
}


void file_utils::append_page_to_table_file(std::string table_file_path, const uint8_t* page){
    FILE* table_file = fopen(table_file_path.c_str(), "a");
    fwrite(page, sizeof(uint8_t), PAGE_SIZE, table_file);
    fclose(table_file);
}


// Write an entire page to the table file, given the page number
void file_utils::write_page_to_table_file(std::string table_file_path, int page_number, const uint8_t *page){
    uint8_t tmp_page[PAGE_SIZE];
    FILE *table_file = fopen(table_file_path.c_str(), "r");
    
    // copy former data
    size_t read_pages = 0;
    while(read_pages < page_number){
        fread(tmp_page, sizeof(uint8_t), PAGE_SIZE, table_file);
        append_page_to_table_file("tmp_file1", tmp_page);
        ++read_pages;
    }
    
    // skip the desired page
    fread(tmp_page, sizeof(uint8_t), PAGE_SIZE, table_file);
    
    // copy latter data
    while(fread(tmp_page, sizeof(uint8_t), PAGE_SIZE, table_file) == PAGE_SIZE){
        append_page_to_table_file("tmp_file2", tmp_page);
    }
    fclose(table_file);
    
    
    table_file = fopen(table_file_path.c_str(), "w");
    // write former data
    FILE* former_file = fopen("tmp_file1", "r");
    while(former_file != NULL && fread(tmp_page, sizeof(uint8_t), PAGE_SIZE, former_file) == PAGE_SIZE){
        fwrite(tmp_page, sizeof(uint8_t), PAGE_SIZE, table_file);
    }
    fclose(former_file);
    fwrite(page, sizeof(uint8_t), PAGE_SIZE, table_file);
    FILE* latter_file = fopen("tmp_file2", "r");
    while(latter_file != NULL && fread(tmp_page, sizeof(uint8_t), PAGE_SIZE, latter_file) == PAGE_SIZE){
        fwrite(tmp_page, sizeof(uint8_t), PAGE_SIZE, table_file);
    }
    fclose(latter_file);
    fclose(table_file);
    
    system("rm -f tmp_file1");
    system("rm -f tmp_file2");
}


// Add a record to a page
int file_utils::add_record_to_page(uint8_t *table_leaf_page, std::pair<uint32_t, std::vector<std::pair<uint8_t, std::string> > > record){
    uint32_t record_id = record.first;
    std::vector<std::pair<uint8_t, std::string> > &record_relation = record.second;
    
    // read header
    uint8_t number_of_existing_records;
    page_read(table_leaf_page, 1, number_of_existing_records);
    uint16_t content_area_boundary;
    page_read(table_leaf_page, 2, content_area_boundary);
    
    
    // check if key already exists
    std::vector<std::pair<uint32_t, uint16_t> > key_loc_pairs;
    for(int i = 0; i < number_of_existing_records; i++){
        uint16_t loc;
        uint32_t key;
        page_read(table_leaf_page, 8 + 2 * i, loc);
        page_read(table_leaf_page, loc + 2, key);
        key_loc_pairs.push_back(std::make_pair(key, loc));
        
        // return code 2 if key already exists
        if(key == record.first){
            std::cout << "[Error] Record with row_id already exists. Try using UPDATE\n";
            return 2;
        }
    }
    
    
    // calculate record expected size
    uint16_t record_size = 6;                   // for header (2 for payload length, 4 for row_id)
    record_size += 1;                           // for number of columns
    record_size += record_relation.size();      // for storing data-types of each
    for(int i = 0; i < record_relation.size(); i++){
        record_size += size_of_type_code(record_relation[i].first);
    }
    
    if(8 + 2 * number_of_existing_records + 6 + record_size > content_area_boundary){
        // return code 1 if unsuccessful because of insufficient space
        return 1;
    }
    uint16_t record_offset = content_area_boundary - record_size;
    key_loc_pairs.push_back(std::make_pair(record.first, record_offset));
    
    // update header
    memset(table_leaf_page + 1, number_of_existing_records + 1, 1);
    memcpy(table_leaf_page + 2, file_utils::byte_pattern(record_offset, 2), 2);
    memcpy(table_leaf_page + 8 + 2 * number_of_existing_records, file_utils::byte_pattern(record_offset, 2), 2);
    
    // add record
    memcpy(table_leaf_page + record_offset, file_utils::byte_pattern(record_size - 6, 2), 2);
    memcpy(table_leaf_page + record_offset + 2, file_utils::byte_pattern(record_id, 4), 4);
    memset(table_leaf_page + record_offset + 6, (int) record_relation.size(), 1);
    size_t offset = record_offset + 7;
    for(int i = 0; i < record_relation.size(); i++){
        memset(table_leaf_page + offset++, record_relation[i].first, 1);
    }
    for(int i = 0; i < record_relation.size(); i++){
        offset = add_value_within_page(table_leaf_page, offset, record_relation[i].second, record_relation[i].first);
    }
    
    // sort the keys
    sort(key_loc_pairs.begin(), key_loc_pairs.end(), comp_key_locs);
    for(int i = 0; i < key_loc_pairs.size(); i++){
        memcpy(table_leaf_page + 8 + 2 * i, file_utils::byte_pattern(key_loc_pairs[i].second, 2), 2);
    }
    
    return 0;
}


size_t file_utils::add_value_within_page(uint8_t *page, size_t offset, std::string data_string, uint8_t type_code){
    switch(type_code){
        case 0x00:{
            uint32_t value = -1;
            memset(page + offset, value, 1);
            return offset + 1;
        }
        case 0x01:{
            uint16_t value = -1;
            memcpy(page + offset, byte_pattern(value, 2), 2);
            return offset + 2;
        }
        case 0x02:{
            uint32_t value = 0xffffffff;
            memcpy(page + offset, byte_pattern(value, 4), 4);
            return offset + 4;
        }
        case 0x03:{
            uint64_t value = -1;
            memcpy(page + offset, byte_pattern(value, 8), 8);
            return offset + 8;
        }
        case 0x04:{
            uint32_t value;
            std::stringstream ss(data_string);
            ss >> value;
            memset(page + offset, value, 1);
            return offset + 1;
        }
        case 0x05:{
            uint16_t value;
            std::stringstream ss(data_string);
            ss >> value;
            memcpy(page + offset, byte_pattern(value, 2), 2);
            return offset + 2;
        }
        case 0x06:{
            uint32_t value;
            std::stringstream ss(data_string);
            ss >> value;
            memcpy(page + offset, byte_pattern(value, 4), 4);
            return offset + 4;
        }
        case 0x07:{
            uint64_t value;
            std::stringstream ss(data_string);
            ss >> value;
            memcpy(page + offset, byte_pattern(value, 8), 8);
            return offset + 8;
        }
        case 0x08:{
            float value = std::stof(data_string);
            uint32_t bin_rep = *((uint32_t*)(&value));
            memcpy(page + offset, byte_pattern(bin_rep, 4), 4);
            return offset + 4;
        }
        case 0x09:{
            double value = std::stod(data_string);
            uint64_t bin_rep = *((uint64_t*)(&value));
            memcpy(page + offset, byte_pattern(bin_rep, 8), 8);
            return offset + 8;
        }
        case 0x0a:{
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

            time_t tval = mktime(&ttm);
            uint64_t time_rep = tval;
            memcpy(page + offset, byte_pattern(time_rep, 8), 8);
            return offset + 8;
        }
        case 0x0b:{
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
            time_t time_rep = mktime(&ttm);
            memcpy(page + offset, byte_pattern(time_rep, 8), 8);
            return offset + 8;
        }
        default:{
            size_t str_size = type_code - 0x0c;
            memcpy(page + offset, data_string.c_str(), str_size);
            return offset + str_size;
        }
    }
    
}



size_t file_utils::read_value_within_page(uint8_t *page, size_t offset, std::string& data_string, uint8_t type_code){
    switch(type_code){
        case 0x00:{
            data_string = "NULL";
            return offset + 1;
        }
        case 0x01:{
            data_string = "NULL";
            return offset + 2;
        }
        case 0x02:{
            data_string = "NULL";
            return offset + 4;
        }
        case 0x03:{
            data_string = "NULL";
            return offset + 8;
        }
        case 0x04:{
            uint8_t value;
            page_read(page, offset, value);
            uint64_t int_value = value;
            data_string = std::to_string(int_value);
            return offset + 1;
        }
        case 0x05:{
            uint16_t value;
            page_read(page, offset, value);
            uint64_t int_value = value;
            data_string = std::to_string(int_value);
            return offset + 2;
        }
        case 0x06:{
            uint32_t value;
            page_read(page, offset, value);
            uint64_t int_value = value;
            data_string = std::to_string(int_value);
            return offset + 4;
        }
        case 0x07:{
            uint64_t value;
            page_read(page, offset, value);
            uint64_t int_value = value;
            data_string = std::to_string(int_value);
            return offset + 8;
        }
        case 0x08:{
            uint32_t value;
            page_read(page, offset, value);
            float float_value = *((float*)(&value));
            data_string = std::to_string(float_value);
            return offset + 4;
        }
        case 0x09:{
            uint64_t value;
            page_read(page, offset, value);
            float double_value = *((double*)(&value));
            data_string = std::to_string(double_value);
            return offset + 8;
        }
        case 0x0a:{
            uint64_t value;
            page_read(page, offset, value);
            time_t time_value = value;
            char str_data[19];
            strftime(str_data, 19, "%Y-%m-%d_%H:%M:%S", localtime(&time_value));
            data_string = std::string(str_data, 19);
            return offset + 8;
        }
        case 0x0b:{
            time_t value;
            page_read(page, offset, value);
            time_t time_value = value;
            char str_data[10];
            strftime(str_data, 10, "%Y-%m-%d", localtime(&time_value));
            data_string = std::string(str_data, 10);
            return offset + 8;
        }
        default:{
            size_t str_size = type_code - 0x0c;
            data_string = std::string((char *)(page + offset), str_size);
            return offset + str_size;
        }
    }
}





#endif /* file_utils_h */
