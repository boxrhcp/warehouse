
#==============================================
#ASSIGNMENT 3 CODE (ASIGNATURA SSOO 2013/2014)
#==============================================

Contents:
- ssoo_p3_concurrency/
	Root folder with p3 contents

- ssoo_p3_concurrency/lib/
	Folder with system libraries

- ssoo_p3_concurrency/lib/libbd_warehouse.a
	Library for managing the database compiled
	on the laboratory machines

- ssoo_p3_concurrency/lib/libbd_warehouse-64bits.a
	Library for managing the databaase compiled
	on a 64-bits machine. Rename to libbd_warehouse.a
	to use

- ssoo_p3_concurrency/include/
	Folder with headers files

- ssoo_p3_concurrency/include/db_warehouse.h
	Headers file for functions to manage warehouse database

- ssoo_p3_concurrency/include/sequential.h
	Headers file with headers to manage the library with same
	behavior as the library to code but without concurrency control
	
- ssoo_p3_concurrency/sequential.c
	File with the code as example with the same behavior
	as the expected but without concurrency control

- ssoo_p3_concurrency/include/concurrent.h
	Headers file with headers expected to be coded 
	to manage the library 

- ssoo_p3_concurrency/concurrent.c
	Base file to code the solution of the assignment, a library
	for concurrent access to the database

- ssoo_p3_concurrency/sequential_example.c
	Example file with calls to the sequential library

- ssoo_p3_concurrency/concurrent_example.c
	Example file with calls to the concurrent library and
	how to properly create threads

- ssoo_p3_concurrency/Makefile
	File to compile all the code (to compile execute "make", 
	to clean the compilation execute "make clean")
