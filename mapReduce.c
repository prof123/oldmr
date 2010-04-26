#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <dirent.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include "mapReduce.h"
#include <sys/dir.h>
#include <dirent.h>

/*Ranks for the global communicator clique
 * Assume mapping tasks numbered from 0 to M-1. 0 is master.
 *reduce tasks from M to M+R-1. 
 *Hence the number of processes given as input is M+R
 */

/*map tasks CAN communicate with all other reducer tasks too.*/
/*choice: let master handle M--R communication or it be independent*/

void mapReduce(int files, char **fnames, int nprocesses, int rprocesses, void *mapfunc, void *reducefunc, void* mergefun)
{
  int numtasks, source=0, dest, tag=1, i, amount;
  char buf[LINE_MAX], **bucketfnames, bucketfname[LINE_MAX];
  MPI_File file, *bucketfiles;
  MPI_Offset size, startpoint;
  int rank;
  bzero(buf, LINE_MAX);
  MPI_Status stat;
  //  MPI_Init(argc,argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

  printf("[%d]: MPI INIT DONE\n",rank);
  int R = rprocesses ;
  HTABLE htable[R];
  for(i = 0; i < R; i++)
    htable[i] = NULL;
  int j=0;

  for(j=0;j<files;j++) {
    if(MPI_File_open(MPI_COMM_WORLD, fnames[j], MPI_MODE_RDONLY | MPI_MODE_UNIQUE_OPEN, MPI_INFO_NULL, &file) < 0) {
      perror("File open");
      exit(0);
  }
  bucketfnames = malloc(rprocesses * sizeof(char *));
  //bucketfiles = malloc(rprocesses * sizeof(MPI_File));

  for(i = 0; i < rprocesses; i++) {
    sprintf(bucketfname, "file%d", i);
    bucketfnames[i] = malloc(strlen(bucketfname)+1);
    sprintf(bucketfnames[i], "%s", bucketfname);
  }

  MPI_File_get_size(file, &size);
  startpoint = size / numtasks * rank;
  	
  if(rank != numtasks - 1)
    amount = size / numtasks * (rank + 1) - startpoint;
  else
    amount = size - startpoint;
  	
  MPI_File_read_at(file, startpoint, buf, amount, MPI_CHAR, &stat);
   
	
  MAP (buf, mapfunc, reducefunc, rprocesses, bucketfnames, stat, rank, numtasks, j, htable); 
  }

  printf("[%d]:The End\n",rank);
  MPI_Barrier( MPI_COMM_WORLD );
  printf("Closing\n");
  MPI_File_close(&file);
  /** Now MERGE*/

  if (rank==0) {
    MERGE(1,2,mergefun) ;
  }

  MPI_Finalize();
}

void MAP (char* input_split,void (*mapfunc)(char**, KV_t*) , void (*reducefunc)(char*, char*), int R, char **bucketfnames, MPI_Status stat, int rank, int numtasks, int j, HTABLE htable[])	/*list of file pointers*/
{  
  //bzero(htable, R);
  int i, r, a;
  char buf[LINE_MAX];

  /*Each Mapper maintains its own hash table*/
  //KV_t *ikv;
  KV_t *retkeyvpair = malloc(sizeof(KV_t));
  retkeyvpair->key = malloc(sizeof(char)*KEYSIZE);
  retkeyvpair->value = malloc(sizeof(char)*KEYSIZE);
  //printf("input = %s", input_split);
  	
  char* input=input_split;

  while(*input!='\0') {
  		
    (*mapfunc)(&input, retkeyvpair); /*Also increments input..*/ 
    retkeyvpair->fileid = j ;   
    printf("MAP [%d] <%s,%s>\n",rank,retkeyvpair->key, retkeyvpair->value);	
    HASH(retkeyvpair,htable, R); /*effectively groups by key*/

  }
  	
  MPI_File files[R];


  char *jpath =(char*)malloc(10);

  itoa(j,jpath) ;

  for(i = 0; i < R; i++) {
  	
    //MPI_File file;
    MPI_Barrier( MPI_COMM_WORLD );


    char *path = strcat(jpath ,bucketfnames[i]) ;
    if(MPI_File_open(MPI_COMM_WORLD, path, MPI_MODE_RDWR | 
		     MPI_MODE_CREATE, MPI_INFO_NULL, &files[i]) < 0) { //MPI_MODE_DELETE_ON_CLOSE
      perror("File open");
      exit(0);
    }
    MPI_Barrier( MPI_COMM_WORLD );
		
    int totalsize = 0;

    if(htable[i] != NULL) {
      struct HT_bucket *temp = htable[i];
  			
      char sbuf[LINE_MAX];
      bzero(buf, LINE_MAX);
      while(temp != NULL) {
  				
	bzero(sbuf, LINE_MAX);
	sprintf(sbuf, "%d", temp->size);  				
	sprintf(&buf[totalsize], "%s", temp->key);
	sprintf(&buf[strlen(temp->key)+1+totalsize], "%d", temp->size);

	for(r = 0; r < temp->size; r++) {
	  if(temp->values[r] == '\0')
	    printf("_");
	  else
	    printf("%c", temp->values[r]);
	}
	printf(", into:&buf[%d], strlen(temp->key):%d, strlen(sbuf):%d\n", strlen(temp->key)+1+strlen(sbuf)+1+totalsize, strlen(temp->key), strlen(sbuf));
	memcpy(&buf[strlen(temp->key)+1+strlen(sbuf)+1+totalsize], temp->values, temp->size);
	totalsize += strlen(temp->key)+1+strlen(sbuf)+1+temp->size;

	temp = temp->next_key;
  				
	printf("[%d], printing unready buffer: ", rank);
  		
	printf("rank = %d, printing buffer: ", rank);
	for(r = 0; r < totalsize; r++) {
	  if(buf[r] == '\0')
	    printf("_");
	  else
	    printf("%c", buf[r]);
	}
	printf("buffer printed\n");
  		
	for(a = 0; a < numtasks; a++) {
	  if(htable[i] != NULL && rank == a) {
	    //MPI_File_set_atomicity(file, 1);
	    printf("bucket = %d, buf = %s\n",i, buf);
	    MPI_File_write_shared(files[i], buf, totalsize, 
				  MPI_CHAR, &stat);
	    //MPI_File_sync(file) ;
	  }
	  MPI_File_sync(files[i]);
	  MPI_Barrier( MPI_COMM_WORLD );
	}
      }
    }
  }
  printf("[%d]: REDUCE ", rank);
		
  for(i = 0; i < R; i++)
    if(rank == i)
      reduce(files[i], stat, reducefunc , j);
		
}

void reduce (MPI_File file, MPI_Status status, void (*reducefunc)(char*, char*), int j)
 {
  int i;
  struct reducer_t* first, *temp;
  intoReduceType(file, &first, status);
/*   printf("All the keys in the end:\n"); */
/*   temp = first; */
/*   while(temp != NULL) { */
			
/*     printf("stored key:%s, size:%d\n", temp->key, temp->size); */
/*     for(i = 0; i < temp->size; i++) */
/*       printf("value[%d]:%s\n", i, temp->vals[i]); */
				
/*     temp = temp->next; */
/*   } */

  /*BEGIN: prateek
   *Now onto the actual reduce. We are finished with grouping by key now***/
  temp=first; //first is the first 'bucket'
  while(temp!=NULL)  {
    int i=0;
    char* reduced_val = temp->vals[0];  //the first value. ?

    for(i=0; i<(temp->size-1); i++) {

      (*reducefunc)(reduced_val, temp->vals[i+1]);
    }

    printf("After reduce KEY:%s , VALUE:%s\n",temp->key,reduced_val);

    /** Now write to the file MERGE/fileid/key */
    reduced_val=strcat(reduced_val,""); //MAJOR FIX
    MPI_File merge_file ;
    char* jp ;
    char* rightp;
    itoa(j, jp);
  
    MPI_Status stat ;
    char* path = "MERGE/";
    path = strcat(path,jp) ;
    path = strcat(path, temp->key) ;
    MPI_File_open(MPI_COMM_WORLD, path, MPI_MODE_RDWR |  MPI_MODE_CREATE, MPI_INFO_NULL, &merge_file) ;
    MPI_File_write_shared(merge_file,reduced_val, strlen(reduced_val),MPI_CHAR, &stat);


  /** move to next key */
    temp=temp->next;
  }


}




int intoReduceType(MPI_File file, struct reducer_t** firstt, MPI_Status status) {
  char buf[LINE_MAX], *bufptr, keybuf[100], sizebuf[100], **vals, valsbuf[100][100], valbuf[100];
  int ret = 0, r, atnow = 0, valsize, values, readbytes, i, a;
  MPI_Offset startpoint = 0, fsize;
  struct reducer_t *temp = NULL, *first = NULL, *next;
	
  bzero(buf, LINE_MAX);
  //MPI_File_set_view( file, 0, MPI_CHAR, MPI_CHAR, "native", MPI_INFO_NULL );
  //MPI_File_read(file, buf, LINE_MAX, MPI_CHAR, &status);
	
  MPI_File_get_size(file, &fsize);
  MPI_File_read_at(file, startpoint, buf, fsize, MPI_CHAR, &status);
	
  printf("printing buffer, size = %d: ", fsize);
  for(r = 0; r < fsize; r++) {
    if(buf[r] == '\0')
      printf("_");
    else
      printf("%c", buf[r]);
  }
  printf("buffer printed\n");
  //exit(0);
  if(strlen(buf) == 0)
    return 0;
	
  bufptr = buf;
  while(1) {
    if((atnow+1) >= fsize)
      break;
		
    //bufptr = &buf[atnow];
		
    bzero(keybuf, 100);
    memcpy(keybuf, bufptr, strlen(bufptr));
    atnow += strlen(keybuf) +1;
    bufptr = &buf[atnow];
		
    bzero(sizebuf, 100);
    memcpy(sizebuf, bufptr, strlen(bufptr));
    atnow += strlen(sizebuf) +1;
    bufptr = &buf[atnow];
		
    valsize = atoi(sizebuf);
    values = 0;
    readbytes = 0;
    while(1) {
      bzero(valbuf, 100);
      memcpy(valbuf, bufptr, strlen(bufptr));
      values++;
      strcpy(valsbuf[values-1], valbuf);
      readbytes += strlen(valbuf)+1;
			
      atnow += strlen(valbuf)+1;
      bufptr = &buf[atnow];
			
      if(readbytes == valsize)
	break;
    }
		
    temp = first;
		
    // This is for the first key.
    if(temp == NULL) {
      temp = malloc(sizeof(struct reducer_t));
      temp->next = NULL;
      temp->size = values;
      temp->key = malloc((strlen(keybuf)+1) * sizeof(char));
      strcpy(temp->key, keybuf);
			
      temp->vals = malloc(values * sizeof(char *));
      for(i = 0; i < values; i++) {
	temp->vals[i] = malloc((strlen(valsbuf[i])+1) * sizeof(char));
	strcpy(temp->vals[i], valsbuf[i]);	
      }
			
      first = temp;			
      continue;
    }
	
    while(1) {
      if(strcmp(temp->key, keybuf) == 0) {
	char **copy = temp->vals;
	temp->vals = malloc((temp->size+values) * sizeof(char *));
	for(i = 0; i < temp->size; i++) {
	  temp->vals[i] = malloc((strlen(copy[i])+1) * sizeof(char));
	  strcpy(temp->vals[i], copy[i]);
	}
	for(a = 0;i < (temp->size+values); a++, i++) {
	  temp->vals[i] = malloc((strlen(valsbuf[a])+1) * sizeof(char));
	  strcpy(temp->vals[i], valsbuf[a]);
	}
	for(i = 0; i < temp->size; i++)
	  free(copy[i]);
	free(copy);
				
	temp->size += values;

	break;
      }
      else if(temp->next == NULL) {
	next = malloc(sizeof(struct reducer_t));
	next->next = NULL;
	next->size = values;
	next->key = malloc((strlen(keybuf)+1) * sizeof(char));
	strcpy(next->key, keybuf);
				
	next->vals = malloc(values * sizeof(char *));
	for(i = 0; i < values; i++) {
	  next->vals[i] = malloc((strlen(valsbuf[i])+1) * sizeof(char));
	  strcpy(next->vals[i], valsbuf[i]);	
	}				
	temp->next = next;
	break;
      }
      temp = temp->next;
    }		
  }
  *firstt = first;	
}


int HASH(KV_t *kv, HTABLE htable[], int R)
{
  /*firstly, determine where key will hash to*/
  int bucket_num = hashfun(kv->key, R);

  char *to_add = kv->value;
  /*to_add->next assigned to previous head of list*/

  /*now figure out the 'top-level' bucket to add it to*/
  struct HT_bucket *buck = htable[bucket_num];
  if(buck==NULL) {
    struct HT_bucket* new = (struct HT_bucket*) malloc(sizeof(struct HT_bucket) );
    new->values = malloc(sizeof(char) * LINE_MAX);
    new->key = malloc(sizeof(char) * (strlen(kv->key)+1));

    strcpy(new->key, kv->key);
    sprintf(new->values, "%s", to_add);
    new->size = strlen(to_add) +1;
    	
    new->next_key=NULL;
    htable[bucket_num] = new;
    	
    return 1;
  }

  struct HT_bucket *temp=buck;
  while(1) {
		
    // Found right key. Adding entry.
    if(strcmp(temp->key, kv->key) == 0) {
			
      sprintf(&(temp->values[temp->size]), "%s", to_add);
      temp->size += strlen(to_add)+1;
      break;
    }
    // To next bucket
    else if(temp->next_key != NULL)
      temp = temp->next_key;
			
    else {			// Adding a new bucket
      struct HT_bucket* new=(struct HT_bucket*) malloc(sizeof(struct HT_bucket) );
      new->values = malloc(sizeof(char) * LINE_MAX);
      new->key = malloc(sizeof(char) * (strlen(kv->key)+1));
      strcpy(new->key, kv->key);
      sprintf(new->values, "%s", to_add);
      new->size = strlen(to_add) +1;
   			
      new->next_key=NULL;
      temp->next_key=new;
      break;
    }
  }
	
  return 1;

} 


 void MERGE(int left, int right , void (*mergefun) (char*, char*)) 
{
  MPI_File merge_file ;
  struct dirent** keylist; 

  char *leftp;
char* rightp;
  itoa(left,leftp) ;
  itoa(right,rightp) ;
  char *lpath = strcat("MERGE/",leftp) ;
  char* rpath = strcat ("MERGE/",rightp) ;
  //  MPI_File_open(MPI_COMM_WORLD, path, MPI_MODE_RDWR |  MPI_MODE_CREATE, MPI_INFO_NULL, &merge_file) ;
  /**use only the local files instead of MPI */

  int count = scandir(lpath, &keylist, NULL,NULL) ;

  struct dirent **curr = keylist ;
  int i =0 ;
  char* lbuff;
  char* rbuff ;

  while (i<count) {

    FILE* lp = fopen(strcat(lpath,keylist[i]->d_name),"r") ;
    FILE* rp = fopen(strcat(rpath,keylist[i]->d_name),"r") ;
    if(rp != NULL) {	//no such file.
      fread(lbuff,1,100,lp) ;
      fread(rbuff,1,100,rp) ;
      printf("key = 	%s  \n", keylist[i]->d_name) ;
      (*mergefun)(lbuff, rbuff) ;
      
    }
    i++ ;
  }
}


/** Simple hash function */

int hashfun(char* str,int R) 
{
  int h=0;
  int g;
  char* p;
  for(p=str;*p!='\0';p++){
    h=(h<<4)+*p;
    if(g=h&0xf0000000) {
      h=h^(g>>24);
      h=h^g;
    }
  }
  return h%R;
}


 void itoa(int n, char s[])
 {
     int i, sign;
 
     if ((sign = n) < 0)  /* record sign */
         n = -n;          /* make n positive */
     i = 0;
     do {       /* generate digits in reverse order */
         s[i++] = n % 10 + '0';   /* get next digit */
     } while ((n /= 10) > 0);     /* delete it */
     if (sign < 0)
         s[i++] = '-';
     s[i] = '\0';
     reverse(s);
 }

 void reverse(char s[])
 {
     int i, j;
     char c;
 
     for (i = 0, j = strlen(s)-1; i<j; i++, j--) {
         c = s[i];
         s[i] = s[j];
         s[j] = c;
     }
 }
