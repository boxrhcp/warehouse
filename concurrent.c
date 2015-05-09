/*
 * Add the rest of necessary "include"
 */
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include "inlcude/db_warehouse.h"
#include "include/concurrent.h"

// Add necessary global variables
pthread_mutex_t meinmutex;														//Mutex to perform the global operations 
pthread_cond_t space;															//Condition to store the threads at the time of performing global operation
int lecturers = MAX_READERS; 													//Integer to check for threads reading the database and to check if there are more than 5 reading at the same time when performing global operations
int writing = 0;																//Integer used as boolean to check if there is any thread writing into the database
/*struct to store the internal of a product*/
typedef struct productataMartino{
	int productw;																//Integer used as boolean to check if there is any thread writing into the product. Will be initialized later, in the concurrent_create_product() function
	int productr;																//Integer to check for threads reading the product and to check if there are more than 5 reading at the same time when performing local operations. Will be initialized later, in the concurrent_create_product() function
	pthread_mutex_t deinmutex;													//Mutex to perform the local operations
	pthread_cond_t products;													//Condition to store the threads at the time of performing local operation
} dataproduct;																	//Name of the struct changed by the typedef flag in the declaration of the struct

/*Function to start the global write synchronization mechanism*/
void global_write_start(){
	pthread_mutex_lock(&meinmutex); 											//Lock to protect changes by many threads to the variable writing
	while(writing==1 || lecturers!=MAX_READERS){ 								//While to check if there is any thread writing in the database or if there is any thread reading the database
		pthread_cond_wait(&space, &meinmutex);									//This wait will suspend the execution of the threads in case there is any other thread writing or reading the database
	}
	writing = 1;																//The value of writing is changed to indicate that a thread is writing the database
	pthread_mutex_unlock(&meinmutex);											//Unlock the mutex once the variable writing is changed
}
/*Function to finish the global write synchronization mechanism once the*/
void global_write_stop(){								
	pthread_mutex_lock(&meinmutex);												//Lock to protect changes by many threads to the variable writing
	writing = 0;																//Set writing to 0 to indicate that the thread has finishing to write into the database
	if(writing==0 && lecturers==MAX_READERS){									//If to check if there are no threads writing on the database or if there are 0 threads reading the database

		pthread_cond_broadcast(&space);											//This broadcast would send a signal to all the threads waiting for the end of the one writing or reading to enter the critical section if there is no thread doing so
	}
	pthread_mutex_unlock(&meinmutex);											//Unlock the mutex once the variable writing is changed
}
/*Function to start the global read synchronization mechanism*/
void global_read_start(){							
	pthread_mutex_lock(&meinmutex);												//Lock to protect changes by many threads to the variable lecturers
	while(writing==1 || lecturers<1){											//While to check if there is any thread writing in the database or if there are 5 threads (the maximum number of threads allowed to read at the same time) reading the database

		pthread_cond_wait(&space, &meinmutex);									//This wait will suspend the execution of the threads in case there is any other thread writing or there are five reading the database
	}
	lecturers--;																//Decrement the value of lecturers to indicate there is a new thread reading the database
	pthread_mutex_unlock(&meinmutex);											//Unlock the mutex once the variable lecturers is changed
}
/*Function to finish the global write synchronization mechanism once the*/
void global_read_stop(){
	pthread_mutex_lock(&meinmutex);												//Lock to protect changes by many threads to the variable lecturers
	lecturers++;																//Increment the value of lecturers to indicate there is a new thread which has finished reading the database
	if (writing==0 && lecturers>0){												//If there are no threads writing into the database and there are at most four threads reading the database, a signal would be sent to one of the threads waiting
	pthread_cond_signal(&space);												//The signal is sent 
	}
	pthread_mutex_unlock(&meinmutex);											//Unlock the mutex once the variable lecturers is changed
}

/*In this function, the database is initialized so the mutex and the conditional variable for the global synchronization mechanism are initialized*/									
int concurrent_init(){									
	int ret;
	pthread_mutex_init(&meinmutex,NULL);
	pthread_cond_init(&space,NULL);

	ret = db_warehouse_init();
 
	return ret;																	//Return 0 if success, -1 if error
}

/*In this function, the database is destroyed so the mutex and the conditional variable for the global synchronization mechanism are destroyed too*/									
int concurrent_destroy(){
	int ret;
	
	pthread_mutex_destroy(&meinmutex);
	pthread_cond_destroy(&space);
		
	ret = db_warehouse_destroy();

	return ret;																	//Return 0 if success, -1 if error
}
/*This function will store a new product on the database, so the global write mechanism is executed to protect the data from writes from other threads and the new product is created by creating 
a new struct initializing its internal data*/
int concurrent_create_product(char *product_name){
	int ret;
	int size;
	void *st_int;

	global_write_start();														//The global write mechanism is executed

	ret = db_warehouse_exists_product(product_name);	
	if (ret == 0){
		ret = db_warehouse_create_product(product_name);
		if (ret == 0){															
		/*If the returned value of "db_warehouse_exists_product(product_name)" and "db_warehouse_create_product(product_name)" memory for the new product is allocated and the internal data of the 
		product, which will follow the same structure as the data for the global reads and writes, is initialized */ 
			dataproduct *feguli = malloc(sizeof(dataproduct));
			feguli->productw = 0;												//"productw" is initialized in 0, as it will be used like the variable "writing" but for the local synchronization mechanism
			feguli->productr = MAX_READERS;										//"productr" is initialized in MAX_READERS (5) as it will be used like the variable "lecturers" but for the local synchronization mechanism
			pthread_mutex_init(&feguli->deinmutex, NULL);						//The mutex "deinmutex" is initialized and it will be used like the "meinmutex" mutex but for the local synchronization mechanism
			pthread_cond_init(&feguli->products, NULL);							//The conditional variable "products" is initialized and it will be used like the "space" condition but for the local synchronization mechanism
			
			size = sizeof(dataproduct);											//The "size" variable, which will be sent as parameter of the "db_warehouse_set_internal_data" function is initialized with the size of the structure
			
			st_int = feguli;													//The "st_int" is initialized with the structure, to be the product sent as parameter of the "db_warehouse_set_internal_data"
			ret = db_warehouse_set_internal_data(product_name, st_int, size);	//The product is stored in the database
		}
	}
	global_write_stop();														//The global write mechanism is finished
	return ret;																	//Return 0 if success, -1 if error
}
/*This function will return the number of products of the database in the variable num_products, so the global read mechanism is executed to protect the data from writes from other threads*/
int concurrent_get_num_products(int *num_products){
	
	int ret;
	int num_products_aux = 0;
	
	global_read_start();											//The global write mechanism is executed
	
	// Obtain number of products from DB using the given library
	ret = db_warehouse_get_num_products(&num_products_aux);

	*num_products = num_products_aux;

	global_read_stop();												//The global write mechanism is finished
	return ret;														//Return 0 if success, -1 if error
}
/*This function will delete a product, so the "db_warehouse_get_internal_data" will return the values to initialize a struct which will be processed to be deleted from the database*/
int concurrent_delete_product(char *product_name){

	int ret, size;
	void *st_int;
	
	global_write_start();													//The global write mechanism is executed
 
	// Read internal data for reset
	ret = db_warehouse_get_internal_data(product_name, &st_int, &size);		//This function will get the data from the product to be deleted
	if (ret == 0){
		/*A new struct will be initialized with the data of the product obtained. The mutex from the product is destroyed, the conditional variable from the product is also destroyed, 
		and its memory is dealocated, the "st_int" and "size" variables are set to NULL and 0, and this values are sent to the database, which will have an empty product*/
		dataproduct *feguli= st_int;
		pthread_mutex_destroy(&feguli->deinmutex);
		pthread_cond_destroy(&feguli->products);
		free(feguli);
		st_int = NULL;
		size = 0;
		// Save the internal data after reset
		ret = db_warehouse_set_internal_data(product_name, st_int, size);
	}
	// Delete product from DB using the given library
	ret = db_warehouse_delete_product(product_name);
    global_write_stop();													//The global write mechanism is finished
    
	return ret;																//Return 0 if success, -1 if error
}
/* This function will increment the stock of a product by reading the data product, which will require the use of the global read synchronization, and the local write synchronization*/
int concurrent_increment_stock(char *product_name, int stock, int *updated_stock){
	int ret, size;
	void *st_int;

	int stock_aux=0;
	
	global_read_start();													//The global read mechanism is executed
	 
	// Obtain internal data to work with them
	ret = db_warehouse_get_internal_data(product_name, &st_int, &size);
	if (ret == 0){
		/*Here starts the local write synchronization mechanism. A new struct will be initialized with the data of the product obtained.*/
		dataproduct *feguli = st_int;
		pthread_mutex_lock(&feguli->deinmutex);								//Lock to protect changes by many threads to the variable "productw"

		while(feguli->productr != MAX_READERS || feguli->productw ==1){		//While loop to make all the threads wait for a thread writing on the product or if there is any thread reading the database
			pthread_cond_wait(&feguli->products, &feguli->deinmutex);		//"wait" to suspend the execution of the threads
		}

		feguli->productw = 1;												//"productw" of the product is set to 1 to indicate that a thread is writing on the product
		pthread_mutex_unlock(&feguli->deinmutex);							//Unlock the mutex of the product
		// Obtain current stock of the product using the given library
		ret = db_warehouse_get_stock(product_name, &stock_aux);

		// Increment stock
		stock_aux += stock;

		// Update stock of the product using the given library
		ret = db_warehouse_update_stock(product_name, stock_aux);
		*updated_stock = stock_aux;

		pthread_mutex_lock(&feguli->deinmutex);								//Lock to protect changes by many threads to the variable "productw"
		feguli->productw = 0;												//"productw" of the product is set to 0 to indicate that a thread is no longer writing on the product

		if(feguli->productr == MAX_READERS && feguli->productw ==0){		//If to check if there are no threads writing on the product or if there are 0 threads reading the product
			pthread_cond_broadcast(&feguli->products);						//This broadcast would send a signal to all the threads waiting for the end of the one writing or reading to enter the critical section if there is no thread doing so
		}
		pthread_mutex_unlock(&feguli->deinmutex);							//Unlock the mutex of the product
		
	}
	global_read_stop();														//The global read mechanism is finished

	return ret;																//Return 0 if success, -1 if error
}
/*This function will decrement the stock of a product by reading the data product, which will require the use of the global read synchronization, and the local write synchronization */
int concurrent_decrement_stock(char *product_name, int stock, int *updated_stock){
	int ret, size;
	void *st_int;

	int stock_aux=0;
	
	global_read_start();													//The global read mechanism is executed
	
	// Obtain internal data to work with them
	ret = db_warehouse_get_internal_data(product_name, &st_int, &size);
	if (ret == 0){
		/*Here starts the local write synchronization mechanism. A new struct will be initialized with the data of the product obtained.*/
		dataproduct *feguli = st_int;
		pthread_mutex_lock(&feguli->deinmutex);								//Lock to protect changes by many threads to the variable "productw"

		while(feguli->productr != MAX_READERS ||  feguli->productw ==1){	//While loop to make all the threads wait for a thread writing on the product or if there is any thread reading the database
			pthread_cond_wait(&feguli->products, &feguli->deinmutex);		//"wait" to suspend the execution of the threads
		}
		feguli->productw = 1;												//"productw" of the product is set to 1 to indicate that a thread is writing on the product
		pthread_mutex_unlock(&feguli->deinmutex);							//Unlock the mutex of the product

		// Obtain current stock of the product using the given library
		ret = db_warehouse_get_stock(product_name, &stock_aux);

		// Decrement stock
		stock_aux -= stock;

		// Update stock of the product using the given library
		ret = db_warehouse_update_stock(product_name, stock_aux);
		*updated_stock = stock_aux;											
		pthread_mutex_lock(&feguli->deinmutex)	;							//Lock to protect changes by many threads to the variable "productw"

		feguli->productw = 0;												//"productw" of the product is set to 0 to indicate that a thread is no longer writing on the product
		if(feguli->productr == MAX_READERS &&  feguli->productw ==0){		//If to check if there are no threads writing on the product or if there are 0 threads reading the product
			pthread_cond_broadcast(&feguli->products);						//This broadcast would send a signal to all the threads waiting for the end of the one writing or reading to enter the critical section if there is no thread doing so
		}
		pthread_mutex_unlock(&feguli->deinmutex);							//Unlock the mutex of the product
	}
	global_read_stop();														//The global read mechanism is finished
	return ret;																//Return 0 if success, -1 if error
}
/*This function will get the stock of a given product usingh the global and the local read synchronization mechanisms*/
int concurrent_get_stock(char *product_name, int *stock){
    int stock_aux=0;
	int ret, size;
	void *st_int;

	global_read_start();													//The global read mechanism is executed
	// Obtain internal data to work with them
	ret = db_warehouse_get_internal_data(product_name, &st_int, &size);
	if (ret == 0){					
		/*Here starts the local read synchronization mechanism. A new struct will be initialized with the data of the product obtained.*/
		dataproduct *feguli = st_int;
		pthread_mutex_lock(&feguli->deinmutex);								//Lock to protect changes by many threads to the variable "productr"

		while(feguli->productw ==1 || feguli->productr <1){					//While to check if there is any thread writing in the product or if there are 5 threads (the maximum number of threads allowed to read at the same time) reading the product
			pthread_cond_wait(&feguli->products, &feguli->deinmutex);		//This wait will suspend the execution of the threads in case there is any other thread writing or there are five reading the product
		}

		feguli->productr--;													//Decrement the value of productr to indicate there is a new thread reading the product
		pthread_mutex_unlock(&feguli->deinmutex);							//Unlock the mutex of the product
		// Obtain current stock from the product using the given library
		ret = db_warehouse_get_stock(product_name, &stock_aux);
		*stock = stock_aux;

		pthread_mutex_lock(&feguli->deinmutex);								//Lock to protect changes by many threads to the variable "productr"
		feguli->productr++;													//Increment the value of productr to indicate there is a new thread which has finished reading the product

		if(feguli->productw ==0 && feguli->productr > 0){					//If there are no threads writing into the product and there are at most four threads reading the product, a signal would be sent to one of the threads waiting	
			pthread_cond_signal(&feguli->products);							//The signal is sent
		}
	pthread_mutex_unlock(&feguli->deinmutex);								//Unlock the mutex of the product
	}
	global_read_stop();														//The global read mechanism is finished

    return ret;																//Return 0 if success, -1 if error
}
