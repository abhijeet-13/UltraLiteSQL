#include <iostream>
#include "parser.h"
#include "file_utils.h"

int main(int argc, const char * argv[]) {
    if(argc > 1 && strcmp(argv[1], "install") == 0){
        Abhi_sql_engine ase;
        ase.install();
        return 0;
    }
    
    as_parser asp;
    asp.launch();
    return 0;
}
