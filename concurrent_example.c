#include <stdio.h>
#include <pthread.h>
#include <sys/time.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "concurrent.h"

/*
 * Constantes con los tiempos de ejecucion de cada operacion
 */

#define TIME_EXISTS_PRODUCT 1
#define TIME_CREATE_PRODUCT_DB 1 
#define GET_NUM_PRODUCTS 4 

#define TIME_CREATE_PRODUCT (TIME_EXISTS_PRODUCT + TIME_CREATE_PRODUCT_DB)
#define TIME_DELETE_PRODUCT 2 
#define TIME_COUNT_PRODUCTS GET_NUM_PRODUCTS 

#define TIME_GET_STOCK 0.75
#define TIME_UPDATE_STOCK 0.75

#define TIME_INCREMENT_STOCK (TIME_GET_STOCK + TIME_UPDATE_STOCK)
#define TIME_DECREMENT_STOCK (TIME_GET_STOCK + TIME_UPDATE_STOCK)

#define THRESHOLD 0.25


#define TRUE 1
#define FALSE 0
#define MAX 5

/*
 * This structure is used to send product name and stock data in
 * only one argument.
 * product: string with product name
 * stock: integer with stock to increment/decremen
 */
struct update_stock_st
{
	char product[128];
	int stock;
};

/**
 * Functions for threads.
 * Operations modifying the warehouse.
 */
 
/*
 * This function, designed to be executed by a thread,
 * creates a product with the given name
 */
void *create_product_thread(void *arg){
	char* product;
	
	product = (char*)malloc(strlen((char*)arg));
	strcpy(product,(char*)arg);
	printf("Thread %s \n",product);
	
	concurrent_create_product(product);
	
	free(product);
	
	pthread_exit (NULL);
}


/*
 * This function, designed to be executed by a thread,
 * deletes a product with the given name
 */
void *delete_product_thread(void *arg){
	char* product;
	
	product = (char*)malloc(strlen((char*)arg));
	strcpy(product,(char*)arg);
	printf("Thread %s \n",product);
	
	concurrent_delete_product(product);
	
	free(product);
	
	pthread_exit (NULL);
}

/*
 * This function, designed to be executed by a thread,
 * counts the number of products in a warehouse
 */
void *count_products_thread(void *arg){
	char* product;
	int num_products;
	
	product = (char*)malloc(strlen((char*)arg));
	strcpy(product,(char*)arg);
	printf("Thread %s \n",product);
	
	concurrent_get_num_products(&num_products);
	
	free(product);
	
	pthread_exit (NULL);
}


/**
 * Functions for threads
 * Product operations
 */

/*
 * This function, designed to be executed by a thread,
 * increment the stock of a product passed by parameter
 * Receives an structure with the product and the stock to increment
 */
void *increment_stock_thread(void *arg){
	struct update_stock_st inc;
	int updated_stock;
	
	memcpy(&inc, arg, sizeof(struct update_stock_st));
	
	concurrent_increment_stock(inc.product, inc.stock, &updated_stock);

	pthread_exit (NULL);
}

/*
 * This function, designed to be executed by a thread,
 * decrements the stock of a product passed by parameter
 * Receives an structure with the product and the stock to decrement
 */
void *decrement_stock_thread(void *arg){
	struct update_stock_st dec;
	int updated_stock;
	
	memcpy(&dec, arg, sizeof(struct update_stock_st)); 
	
	concurrent_decrement_stock(dec.product, dec.stock, &updated_stock);

	pthread_exit (NULL);
}

/*
 * This function, designed to be executed by a thread,
 * obtains the stock of a given product
 */
void *get_stock_thread(void *arg){
	char* product;
	int stock;
	
	product = (char*)malloc(strlen((char*)arg));
	strcpy(product,(char*)arg);
	
	concurrent_get_stock(product, &stock);
	
	free(product);

	pthread_exit (NULL);
}


/**
 * TESTS
 */

/*
 * In this test, 5 write operations in the warehouse are launched
 * concurrently. Five products are created
 */
int testWWWWWcreate(){
	int i;
	pthread_t th_test[MAX];
	void *st1;
	char product[MAX][128];
	struct timeval start_time,end_time;
	double start,end;
	int res;
	int expected_time;
	
	printf("\n\n");
	printf("*************************************** \n");
	printf("************ TEST WWWWW1 *********** \n");
	printf("*************************************** \n");
	
	// Measure start time of the test
	gettimeofday(&start_time, NULL);
	start = start_time.tv_sec + (start_time.tv_usec / 1000000.0);
	
	// Create 5 threads for operations over the warehouse
	for (i=0;i<MAX;i++){
		sprintf(product[i], "product%d", i);
		pthread_create (&th_test[i], NULL, create_product_thread, product[i]);
	}
	// Join the 5 threads (wait for their finish)
	for (i=0;i<MAX;i++){
		pthread_join (th_test[i], (void **) &st1);
	}
	
	// Measure end time
	gettimeofday(&end_time, NULL);
	end = end_time.tv_sec + (end_time.tv_usec / 1000000.0);
	
	// Calculate expected time: 5 writes should be sequential
	expected_time = TIME_CREATE_PRODUCT*MAX;
	
	printf("*************************************** \n");
	if(end-start > expected_time && end-start < expected_time+THRESHOLD){
		printf("********** TEST WWWWW1 OK! ********* \n");
		res = TRUE;
	}else{
		printf("********** TEST WWWWW1 ERR ********* \n");
		res = FALSE;
	}
	printf("*************************************** \n");
	printf("********** END TEST WWWWW1 ********* \n");
	printf("*************************************** \n");
	
	return res;
}


/*
 * In this test, 5 write operations in the warehouse are launched
 * concurrently. Five products are deleted.
 */
int testWWWWWdelete(){
	int i;
	pthread_t th_test[MAX];
	void *st1;
	char product[MAX][128];
	struct timeval start_time,end_time;
	double start,end;
	int res;
	int expected_time;
	
	printf("\n\n");
	printf("*************************************** \n");
	printf("************ TEST WWWWW2 *********** \n");
	printf("*************************************** \n");
	
	// Measure start time of the test
	gettimeofday(&start_time, NULL);
	start = start_time.tv_sec + (start_time.tv_usec / 1000000.0);
	
	// Create 5 threads for operations over the warehouse
	for (i=0;i<MAX;i++){
		sprintf(product[i], "product%d", i);
		pthread_create (&th_test[i], NULL, delete_product_thread, product[i]);
	}
	// Join the 5 threads (wait for their finish)
	for (i=0;i<MAX;i++){
		pthread_join (th_test[i], (void **) &st1);
	}
	
	// Measure end time
	gettimeofday(&end_time, NULL);
	end = end_time.tv_sec + (end_time.tv_usec / 1000000.0);
	
	// Calculate expected time: 5 writes should be sequential
	expected_time = TIME_DELETE_PRODUCT*MAX;
	
	printf("*************************************** \n");
	if(end-start > expected_time && end-start < expected_time+THRESHOLD){
		printf("********** TEST WWWWW2 OK! ********* \n");
		res = TRUE;
	}else{
		printf("********** TEST WWWWW2 ERR ********* \n");
		res = FALSE;
	}
	printf("*************************************** \n");
	printf("********** END TEST WWWWW2 ********* \n");
	printf("*************************************** \n");
	
	return res;
}


/*
 * In this test, 5 read operations in the warehouse are launched
 * concurrently. Five products are deleted.
 */
int testRRRRR(){
	int i;
	pthread_t th_test[MAX];
	void *st1;
	char product[MAX][128];
	struct timeval start_time,end_time;
	double start,end;
	int res;
	int expected_time;
	
	printf("\n\n");
	printf("*************************************** \n");
	printf("************ TEST RRRRR ************ \n");
	printf("*************************************** \n");
	
	// Measure start time of the test
	gettimeofday(&start_time, NULL);
	start = start_time.tv_sec + (start_time.tv_usec / 1000000.0);
	
	// Create 5 threads for operations over the warehouse
	for (i=0;i<MAX;i++){
		sprintf(product[i], "product%d", i);
		pthread_create (&th_test[i], NULL, count_products_thread, product[i]);
	}
	// Join the 5 threads (wait for their finish)
	for (i=0;i<MAX;i++){
		pthread_join (th_test[i], (void **) &st1);
	}
	
	// Measure end time
	gettimeofday(&end_time, NULL);
	end = end_time.tv_sec + (end_time.tv_usec / 1000000.0);
	
	// The 6 read operations should be don in parallel
	// (the time of 6 reads should be similar to 1)
	expected_time = TIME_COUNT_PRODUCTS;
	
	printf("*************************************** \n");
	if(end-start > expected_time && end-start < expected_time+THRESHOLD){
		printf("********** TEST RRRRR OK! ********** \n");
		res = TRUE;
	}else{
		printf("********** TEST RRRRR ERR ********** \n");
		res = FALSE;
	}
	printf("*************************************** \n");
	printf("********** END TEST RRRRR ********** \n");
	printf("*************************************** \n");
	
	return res;
}

/*
 * In this test, 4 write operations in the warehouse are launched
 * concurrently followed by 2 read operations.
 */
int testWWWWRRcreate(){
	int i;
	pthread_t th_test[MAX+1];
	void *st1;
	char product[MAX+1][128];
	struct timeval start_time,end_time;
	double start,end;
	int res;
	int expected_time;
	
	printf("\n\n");
	printf("*************************************** \n");
	printf("************ TEST WWWWRR1 *********** \n");
	printf("*************************************** \n");
	
	// Measure start time of the test
	gettimeofday(&start_time, NULL);
	start = start_time.tv_sec + (start_time.tv_usec / 1000000.0);
	
	// Create 6 threads (4 for writes, 2 for reads)
	// Sleep 1 second between threads to assure the correct order
	for (i=0;i<MAX+1;i++){
		sprintf(product[i], "product%d", i);
	}
	pthread_create (&th_test[0], NULL, create_product_thread, product[0]);
	sleep(1);
	pthread_create (&th_test[1], NULL, create_product_thread, product[1]);
	sleep(1);
	pthread_create (&th_test[2], NULL, create_product_thread, product[2]);
	sleep(1);
	pthread_create (&th_test[3], NULL, create_product_thread, product[3]);
	sleep(1);
	pthread_create (&th_test[4], NULL, count_products_thread, product[4]);
	pthread_create (&th_test[5], NULL, count_products_thread, product[5]);
	// Join the 6 threads (wait for their finish)
	for (i=0;i<MAX+1;i++){
		pthread_join (th_test[i], (void **) &st1);
	}
	
	// Measure end time
	gettimeofday(&end_time, NULL);
	end = end_time.tv_sec + (end_time.tv_usec / 1000000.0);
	
	// Write operations should be sequential (4xWRITE), 
	// read operations should be concurrent (1xREAD)
	// Sleeps are masked by writes
	expected_time = TIME_CREATE_PRODUCT*4 + TIME_COUNT_PRODUCTS;
	
	printf("*************************************** \n");
	printf("*************************************** \n");
	if(end-start > expected_time && end-start < expected_time+THRESHOLD){
		printf("********** TEST WWWWRR1 OK! ********* \n");
		res = TRUE;
	}else{
		printf("********** TEST WWWWRR1 ERR ********* \n");
		res = FALSE;
	}
	printf("*************************************** \n");
	printf("********** END TEST WWWWRR1 ********* \n");
	printf("*************************************** \n");
	
	return res;
}

/*
 * In this test, 4 write operations in the warehouse are launched
 * concurrently followed by 2 read operations.
 */
int testWWWWRRdelete(){
	int i;
	pthread_t th_test[MAX+1];
	void *st1;
	char product[MAX+1][128];
	struct timeval start_time,end_time;
	double start,end;
	int res;
	int expected_time;
	
	printf("\n\n");
	printf("*************************************** \n");
	printf("************ TEST WWWWRR2 *********** \n");
	printf("*************************************** \n");
	
	// Measure start time of the test
	gettimeofday(&start_time, NULL);
	start = start_time.tv_sec + (start_time.tv_usec / 1000000.0);
	
	// Create 6 threads (4 for writes, 2 for reads)
	// Sleep 1 second between threads to assure the correct order
	for (i=0;i<MAX+1;i++){
		sprintf(product[i], "product%d", i);
	}
	pthread_create (&th_test[0], NULL, delete_product_thread, product[0]);
	sleep(1);
	pthread_create (&th_test[1], NULL, delete_product_thread, product[1]);
	sleep(1);
	pthread_create (&th_test[2], NULL, delete_product_thread, product[2]);
	sleep(1);
	pthread_create (&th_test[3], NULL, delete_product_thread, product[2]);
	sleep(1);
	pthread_create (&th_test[4], NULL, count_products_thread, product[4]);
	pthread_create (&th_test[5], NULL, count_products_thread, product[5]);
	// Join the 6 threads (wait for their finish)
	for (i=0;i<MAX+1;i++){
		pthread_join (th_test[i], (void **) &st1);
	}
	
	// Measure end time
	gettimeofday(&end_time, NULL);
	end = end_time.tv_sec + (end_time.tv_usec / 1000000.0);
	
	// Write operations should be sequential (4xWRITE), 
	// read operations should be concurrent (1xREAD)
	// Sleeps are masked by writes
	expected_time = TIME_DELETE_PRODUCT*4 + TIME_COUNT_PRODUCTS;
	
	printf("*************************************** \n");
	if(end-start > expected_time && end-start < expected_time+THRESHOLD){
		printf("********** TEST WWWWRR2 OK! ********* \n");
		res = TRUE;
	}else{
		printf("********** TEST WWWWRR2 ERR ********* \n");
		res = FALSE;
	}
	printf("*************************************** \n");
	printf("********** END TEST WWWWRR2 ********* \n");
	printf("*************************************** \n");
	
	return res;
}


/*
 * In this test, 4 read operations in the warehouse are launched
 * concurrently followed by 2 write operations.
 */
int testRRRRWWcreate(){
	int i;
	pthread_t th_test[MAX+1];
	void *st1;
	char product[MAX+1][128];
	struct timeval start_time,end_time;
	double start,end;
	int res;
	int expected_time;
	
	printf("\n\n");
	printf("*************************************** \n");
	printf("************ TEST RRRRWW1 *********** \n");
	printf("*************************************** \n");
	
	// Measure start time of the test
	gettimeofday(&start_time, NULL);
	start = start_time.tv_sec + (start_time.tv_usec / 1000000.0);
	
	// Create 6 threads (4 for reads, 2 for writes)
	// Sleep 1 second between threads to assure the correct order
	for (i=0;i<MAX+1;i++){
		sprintf(product[i], "product%d", i);
	}
	pthread_create (&th_test[0], NULL, count_products_thread, product[0]);
	sleep(1);
	pthread_create (&th_test[1], NULL, count_products_thread, product[1]);
	sleep(1);
	pthread_create (&th_test[2], NULL, count_products_thread, product[2]);
	sleep(1);
	pthread_create (&th_test[3], NULL, count_products_thread, product[3]);
	sleep(1);
	pthread_create (&th_test[4], NULL, create_product_thread, product[4]);
	sleep(1);
	pthread_create (&th_test[5], NULL, create_product_thread, product[5]);
	sleep(1);
	// Join the 5 threads (wait for their finish)
	for (i=0;i<MAX+1;i++){
		pthread_join (th_test[i], (void **) &st1);
	}
	
	// Measure end time
	gettimeofday(&end_time, NULL);
	end = end_time.tv_sec + (end_time.tv_usec / 1000000.0);
	
	// Read operations should be simultaneous, but
	// the last one starts 3 seconds later than the first one (because of sleep times).
	// Write operations should be done after every read an sequentially
	expected_time = (TIME_COUNT_PRODUCTS + 3) + TIME_CREATE_PRODUCT*2;
	
	printf("*************************************** \n");
	if(end-start > expected_time && end-start < expected_time+THRESHOLD){
		printf("********** TEST RRRRWW1 OK! ********* \n");
		res = TRUE;
	}else{
		printf("********** TEST RRRRWW1 ERR ********* \n");
		res = FALSE;
	}
	printf("*************************************** \n");
	printf("********** END TEST RRRRWW1 ********* \n");
	printf("*************************************** \n");
	
	return res;
}


/*
 * In this test, 4 read operations in the warehouse are launched
 * concurrently followed by 2 write operations.
 */
int testRRRRWWdelete(){
	int i;
	pthread_t th_test[MAX+1];
	void *st1;
	char product[MAX+1][128];
	struct timeval start_time,end_time;
	double start,end;
	int res;
	int expected_time;
	
	printf("\n\n");
	printf("*************************************** \n");
	printf("************ TEST RRRRWW2 *********** \n");
	printf("*************************************** \n");
	
	// Measure start time of the test
	gettimeofday(&start_time, NULL);
	start = start_time.tv_sec + (start_time.tv_usec / 1000000.0);
	
	// Create 6 threads (4 for reads, 2 for writes)
	// Sleep 1 second between threads to assure the correct order
	for (i=0;i<MAX+1;i++){
		sprintf(product[i], "product%d", i);
	}
	pthread_create (&th_test[0], NULL, count_products_thread, product[0]);
	sleep(1);
	pthread_create (&th_test[1], NULL, count_products_thread, product[1]);
	sleep(1);
	pthread_create (&th_test[2], NULL, count_products_thread, product[2]);
	sleep(1);
	pthread_create (&th_test[3], NULL, count_products_thread, product[3]);
	sleep(1);
	pthread_create (&th_test[4], NULL, delete_product_thread, product[4]);
	sleep(1);
	pthread_create (&th_test[5], NULL, delete_product_thread, product[5]);
	sleep(1);
	// Join the 5 threads (wait for their finish)
	for (i=0;i<MAX+1;i++){
		pthread_join (th_test[i], (void **) &st1);
	}
	
	// Measure end time
	gettimeofday(&end_time, NULL);
	end = end_time.tv_sec + (end_time.tv_usec / 1000000.0);
	
	// Read operations should be simultaneous, but
	// the last one starts 3 seconds later than the first one (because of sleep times).
	// Write operations should be done after every read an sequentially
	expected_time = (TIME_COUNT_PRODUCTS + 3) + TIME_DELETE_PRODUCT*2;
	
	printf("*************************************** \n");
	if(end-start > expected_time && end-start < expected_time+THRESHOLD){
		printf("********** TEST RRRRWW2 OK! ********* \n");
		res = TRUE;
	}else{
		printf("********** TEST RRRRWW2 ERR ********* \n");
		res = FALSE;
	}
	printf("*************************************** \n");
	printf("********** END TEST RRRRWW2 ********* \n");
	printf("*************************************** \n");
	
	return res;
}


/* Main function to execute */
int main(){
	//int i = 0;
	// Init the DB
	concurrent_init();

	// Launch desired tests
	// NOTE: to left the DB in a stable state one delete test should be called after each create test
	// example: testWWWWWcreate followed by testWWWWWdelete 
	testWWWWWcreate();
	testWWWWWdelete();
	testRRRRR();
	testWWWWRRcreate();
	testWWWWRRdelete();
	testRRRRWWcreate();
	testRRRRWWdelete();
	/*
	for(i = 0; i < 1000; i++){
		if(i % 100 == 0){
			fprintf(stderr,"i: %d \n",i);
		}
		if(testWWWWRRcreate() != TRUE){
			fprintf(stderr,"ERROR!!!!!!!!!!!!!!!!!!!!!! \n");
		}
		if(testWWWWRRdelete() != TRUE){
			fprintf(stderr,"ERROR!!!!!!!!!!!!!!!!!!!!!! \n");
		}
	}*/
	
	
	// Destroy DB
	concurrent_destroy();

	return 0;
}
