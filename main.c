/*Map Reduce parallel implementation using MPI. C code which can execute paralle *ly on cluster parallely.
 *First done as part of the parallel computing course (April 2008).
 */
/*Design notes:
 *1.File split to give to individual map tasks
 *2.Mappers execute on their own, and group by key locally. (This is hopefully all done in local memory, since max amt of data given to mapper task is ~ 64 Mb even in google's implementation
 *3.??
 *4. Reduce. (profit!!!!)
 */


#include "mapReduce.h"

void mapfunc(char** foo, KV_t *kv);
void reducefunc();
void mergefun(char* lbuff, char* rbuff) ;


int main(int argc, char **argv) {
  char **fnames; 
  int nfiles, rprocesses;
  //	nfiles=1;
  rprocesses = atoi(argv[1]);

  MPI_Init(&argc,&argv);

  fnames = argv+2;
  nfiles = argc - 2 ;
  mapReduce(nfiles, fnames, 10, rprocesses, mapfunc, reducefunc, mergefun);
	
  //void mapReduce(int files, char **fnames, int nprocesses, int rprocesses,
  //void *mapfunc, void *reducefunc);

	
  return 0;
}

void mapfunc(char** foo, KV_t *kv) {
  char* c=(char*)*foo;
  sprintf(kv->key, "%c", *c);
  sprintf(kv->value, "1");
  (*foo)++;
}

void reducefunc(char* inout, char* in) {
  int i=atoi(in);
  int j=atoi(inout);
  sprintf(inout, "%d", (i+j));
}

void mergefun(char* lbuff, char* rbuff) {
  printf("LEFT value: %s \n",lbuff) ;
  printf("RIGHT value: %s \n",rbuff) ;

}
