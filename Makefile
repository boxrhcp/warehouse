#--------------------------
# Variables
#--------------------------
#
MAKE         = make -s
CC           = gcc
SOURCE_PATH  = ./
OBJECTS      = sequential_example.c concurrent_example.c concurrent.c sequential.c
EXE          = sequential_example.exe concurrent_example.exe

INCLUDE_BASE = ./include/
LIB_BASE     = ./lib/

MYFLAGS      = -Wall -g
MYFLAGSBD	 = 

MYHEADERS    =  -I${INCLUDE_BASE} 
MYLIBS       =  -L${LIB_BASE} -ldb_warehouse -lpthread


#--------------------------
# Compilation rules                                           
#--------------------------                                   
#                                                               
#                                                                                                                             
all:  $(OBJECTS)
	@echo "                              Compiling "
	@echo -n "                              "
	@echo -n "Concurent example [.."
	@$(CC) concurrent_example.c concurrent.c $(MYFLAGS) $(MYHEADERS) $(MYLIBS) -o concurrent_example.exe
	@echo "..]"
	@echo "                              Example compiled successfully!"
	@echo ""
	@echo "                              Compiling "
	@echo -n "                              "
	@echo -n "Sequential Example [.."
	@$(CC) sequential_example.c sequential.c $(MYFLAGS) $(MYHEADERS) $(MYLIBS) -o sequential_example.exe
	@echo "..]"
	@echo "                              Example compiled successfully!"
                                                              
                                                              
clean:
	@echo "                              Deleted files!"
	@echo -n "                              "
	@rm -f *.o *exe 
	@echo  ""
        
