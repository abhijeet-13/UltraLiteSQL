#ifndef bplus_tree_hpp
#define bplus_tree_hpp

#include <iostream>
#include <vector>
#include "file_utils.h"
using namespace std;

#define MAX_NODES_ALLOWED 48

class btree_utils{
    
public:

    static pair<uint32_t, int32_t> btree_insert_util(string table_file_path, uint32_t root_page_addr, record_type &record){
        uint32_t record_key = record.first;
        string value_string;
        
        // read the page from the table file
        uint8_t root_page[PAGE_SIZE];
        file_utils::read_page_from_table_file(table_file_path, root_page_addr / PAGE_SIZE, root_page);
        
        // gather some general info about the root
        int8_t number_of_records = root_page[1];
        file_utils::read_value_within_page(root_page, 2, value_string, 0x05);
        uint16_t content_offset = (uint16_t) stol(value_string);
        //size_t remaining_bytes = content_offset - (8 + 2 * number_of_records);
        
        // if it's a btree leaf page
        if(root_page[0] == 0x0d){
            
            int status = file_utils::add_record_to_page(root_page, record);
            if(status == 0 || status == 2){
                file_utils::write_page_to_table_file(table_file_path, root_page_addr / PAGE_SIZE, root_page);
                return make_pair(-1, -1);
            }
            
            // if no more space, split below
            uint32_t new_page_addr = file_utils::append_page_to_table_file(table_file_path);
            uint8_t new_page[PAGE_SIZE];
            file_utils::read_page_from_table_file(table_file_path, new_page_addr / PAGE_SIZE, new_page);
            
            // add new in linked list
            uint32_t root_right;
            file_utils::page_read(root_page, 4, root_right);
            file_utils::add_value_within_page(new_page, 4, to_string(root_right), 0x06);
            file_utils::add_value_within_page(root_page, 4, to_string(new_page_addr), 0x06);
            
            
            // find where the new key goes
            bool KeyInLeft;
            uint32_t middleKey;
            uint16_t middleKeyOffset;
            file_utils::page_read(root_page, 8 + 2 * (number_of_records / 2), middleKeyOffset);
            file_utils::page_read(root_page, middleKeyOffset + 2, middleKey);
            size_t left_number_of_records, right_number_of_records;
            if(record_key <= middleKey){
                KeyInLeft = true;
                left_number_of_records = (number_of_records / 2) - 1 + 1;
            }
            else{
                KeyInLeft = false;
                left_number_of_records = (number_of_records / 2) + 1;
            }
            right_number_of_records = number_of_records - left_number_of_records;
            
            
            // update left node
            root_page[1] = left_number_of_records;
            uint16_t new_offset;
            file_utils::page_read(root_page, 8 + 2 * (left_number_of_records - 1), new_offset);
            file_utils::add_value_within_page(root_page, 2, to_string(new_offset), 0x05);
            
            new_page[1] = right_number_of_records;
            uint16_t right_content_offset;
            file_utils::page_read(new_page, 2, right_content_offset);
            for(int i = 0; i < right_number_of_records; i++){
                uint16_t tmp_offset;
                file_utils::page_read(root_page, 8 + 2 * (left_number_of_records + i), tmp_offset);
                uint16_t payload_size;
                file_utils::page_read(root_page, tmp_offset, payload_size);
                right_content_offset -= (payload_size + 6);
                memcpy(new_page + right_content_offset, root_page + tmp_offset, payload_size + 6);
                file_utils::add_value_within_page(new_page, 2, to_string(right_content_offset), 0x05);
                file_utils::add_value_within_page(new_page, 8 + 2 * i, to_string(right_content_offset), 0x05);
            }
            

            if(KeyInLeft){
                status = file_utils::add_record_to_page(root_page, record);
            }
            else{
                status = file_utils::add_record_to_page(new_page, record);
            }
            file_utils::write_page_to_table_file(table_file_path, root_page_addr / PAGE_SIZE, root_page);
            file_utils::write_page_to_table_file(table_file_path, new_page_addr / PAGE_SIZE, new_page);
            
            if(status == 0){
                // find max in left
                uint16_t tmp_offset;
                uint32_t left_max_val;
                file_utils::page_read(root_page, 8 + 2 * (root_page[1] - 1), tmp_offset);
                file_utils::page_read(root_page, tmp_offset + 2, left_max_val);
                return make_pair(left_max_val, new_page_addr);
            }
            else{
                return make_pair(-1, -1);
            }
        }
        
        // if it's a btree internal page
        else if(root_page[0] == 0x05){
            
            // find the branch to recurse on
            uint32_t branch_addr;
            uint16_t offset = 0;
            uint32_t key;
            int loc = 0;
            while(loc < number_of_records){
                file_utils::read_value_within_page(root_page, 8 + 2 * loc, value_string, 0x05);
                offset = stoi(value_string);
                file_utils::read_value_within_page(root_page, offset + 4, value_string, 0x06);
                key = stoi(value_string);

                // advance if key is less than target key, else break
                if(key >= record_key)
                    break;
                ++loc;
            }
            if(loc < number_of_records){
                file_utils::read_value_within_page(root_page, offset, value_string, 0x04);
                branch_addr = stoi(value_string);
            }
            else{
                file_utils::read_value_within_page(root_page, 4, value_string, 0x06);
                branch_addr = stoi(value_string);
            }
            
            // recurse on that branch address
            pair<uint32_t, uint32_t> return_val = btree_insert_util(table_file_path, branch_addr, record);
            if(return_val.second == -1)
                return return_val;
            
            // check if more nodes can be added
            if(number_of_records < MAX_NODES_ALLOWED){
                // add the .first at loc
                
                // update existing loc's left pointer
                uint16_t tmp_offset;
                if(loc >= number_of_records){
                    file_utils::add_value_within_page(root_page, 4, to_string(return_val.second), 0x06);
                }
                else{
                    file_utils::page_read(root_page, 8 + 2 * loc, tmp_offset);
                    file_utils::add_value_within_page(root_page, tmp_offset, to_string(return_val.second), 0x06);
                }

                // add the cell for new node
                ++root_page[1];
                file_utils::add_value_within_page(root_page, 2, to_string(content_offset - 8), 0x05);
                file_utils::add_value_within_page(root_page, content_offset - 8, to_string(branch_addr), 0x06);
                file_utils::add_value_within_page(root_page, content_offset - 4, to_string(return_val.first), 0x06);
                
                // shift all the nodes after loc to right and fill loc with (current_offset - 8)
                for(int i = number_of_records - 1; i >= loc; i--){
                    memcpy(root_page + 8 + 2 * (i + 1), root_page + 8 + 2 * i, 2);
                }
                file_utils::add_value_within_page(root_page, 8 + 2 * loc, to_string(content_offset - 8), 0x05);
                
                file_utils::write_page_to_table_file(table_file_path, root_page_addr / PAGE_SIZE, root_page);
                return make_pair(-1, -1);
            }
            else{
                // TODO: split internal node
                return make_pair(-1, -1);
            }
        }
        
        // if unrecognized btree page
        else{
            //cout << "Huh? Wait, what? Huh?\n";
            return make_pair(-1, -1);
        }
    }


    static void btree_insert(string table_file_path, record_type &record){
        uint32_t original_root_page_addr;
        fstream f;
        string table_root_path = table_file_path;
        table_root_path[table_root_path.size() - 1] = 'r';
        f.open(table_root_path, ios::in);
        f >> original_root_page_addr;
        f.close();
        
        pair<uint32_t, int32_t> return_val = btree_insert_util(table_file_path, original_root_page_addr, record);
        if(return_val.first == -1)
            return;
        else{
            uint32_t root_page_addr = file_utils::append_page_to_table_file(table_file_path, 0x05);
            uint8_t root_page[PAGE_SIZE];
            file_utils::read_page_from_table_file(table_file_path, root_page_addr / PAGE_SIZE, root_page);
            root_page[1] = 1;
            file_utils::add_value_within_page(root_page, 2, to_string(PAGE_SIZE - 8), 0x05);
            file_utils::add_value_within_page(root_page, 8, to_string(PAGE_SIZE - 8), 0x05);
            file_utils::add_value_within_page(root_page, PAGE_SIZE - 8, to_string(original_root_page_addr), 0x06);
            file_utils::add_value_within_page(root_page, PAGE_SIZE - 4, to_string(return_val.first), 0x06);
            file_utils::add_value_within_page(root_page, 4, to_string(return_val.second), 0x06);
            file_utils::write_page_to_table_file(table_file_path, root_page_addr / PAGE_SIZE, root_page);
            
            // update root page addr
            f.open(table_root_path, ios::out);
            f << root_page_addr;
            f.close();
        }
    }
    
};


#endif /* bplus_tree_hpp */
