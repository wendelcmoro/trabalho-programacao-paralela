#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

// MAX char table (ASCII)
#define MAX 256

#define STD_TAG 0

// Boyers-Moore-Hospool-Sunday algorithm for string matching
int bmhs(char *string, int n, char *substr, int m) {

	int d[MAX];
	int i, j, k;

	// pre-processing
	for (j = 0; j < MAX; j++)
		d[j] = m + 1;
	for (j = 0; j < m; j++)
		d[(int) substr[j]] = m - j;

	// searching
	i = m - 1;
	while (i < n) {
		k = i;
		j = m - 1;
		while ((j >= 0) && (string[k] == substr[j])) {
			j--;
			k--;
		}
		if (j < 0)
			return k + 1;
		i = i + d[(int) string[i + 1]];
	}

	return -1;
}

FILE *fdatabase, *fquery, *fout;

void openfiles() {

	fdatabase = fopen("dna.in", "r+");
	if (fdatabase == NULL) {
		perror("dna.in");
		exit(EXIT_FAILURE);
	}

	fquery = fopen("query.in", "r");
	if (fquery == NULL) {
		perror("query.in");
		exit(EXIT_FAILURE);
	}

	fout = fopen("dna.out", "w");
	if (fout == NULL) {
		perror("fout");
		exit(EXIT_FAILURE);
	}

}

void closefiles() {
	fflush(fdatabase);
	fclose(fdatabase);

	fflush(fquery);
	fclose(fquery);

	fflush(fout);
	fclose(fout);
}

void remove_eol(char *line) {
	int i = strlen(line) - 1;
	while (line[i] == '\n' || line[i] == '\r') {
		line[i] = 0;
		i--;
	}
}

char *bases;
char *str;

// To run mpi program execute: mpirun --oversubscribe -np num_procs PROGRAM_NAME
int main(int argc, char **argv) {
	double start = MPI_Wtime();
	int my_rank, n_procs;

    n_procs = 6;
	MPI_Status status;

	MPI_Init(&argc, &argv);	
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &n_procs);

	int ch = 0;
	long long int lines = 0;

	int i, found, result;

	char line[100];	

	long long int fileSize = 0;
	long long int queryLines = 0;

	// pre-alocate the bases in the memory
	str = (char*) malloc(1000001 * sizeof(char));
	if (str == NULL) {
		perror("malloc");
		exit(EXIT_FAILURE);
	}

	if(my_rank == 0) {
		openfiles();		

		int offset = 0;
		int iLine = 0;		

		// count the number of bases contained in dna.in
		while(!feof(fdatabase)) {
			ch = fgetc(fdatabase);
			if(ch == '>') {
				lines += 2;
			}
		}

		// pre-alocate the bases in the memory
		bases = (char*) malloc(lines * 1000001 * sizeof(char));
		if (bases == NULL) {
			perror("malloc");
			exit(EXIT_FAILURE);
		}

		// save database on bases array
		fseek(fdatabase, 0, SEEK_SET);
		fgets(line, 100, fdatabase);
		remove_eol(line);
		while (!feof(fdatabase)) {
			strcpy(bases + offset, line);
			iLine++;

			offset = iLine * 1000001;
			bases[offset] = 0;
			i = 0;
			fgets(line, 100, fdatabase);
			remove_eol(line);
			do {
				strcat(bases + offset + i, line);
				if (fgets(line, 100, fdatabase) == NULL)
					break;
				remove_eol(line);
				i += 80;
			} while (line[0] != '>');
			iLine++;
			offset = iLine * 1000001;
		}
		// count the number of queries contained in query.in
		while(!feof(fquery)) {
			ch = fgetc(fquery);
			fileSize++;
			if(ch == '>') {
				queryLines++;
			}
		}

		// pre-allocate the queries into memory
		char *msg  = (char*) malloc(sizeof(char) * fileSize);
		if (msg== NULL) {
			perror("malloc msg");
			exit(EXIT_FAILURE);
		}
		
		// output array string for merging the results
		char *output  = (char*) malloc(sizeof(char) * fileSize);
		if (output== NULL) {
			perror("malloc msg");
			exit(EXIT_FAILURE);
		}

		char collum[11];
		output[0] = 0;
		collum[0] = 0;
		msg[0] = 0;

		// save the queries into memory
		i = 0;
		fseek(fquery, 0, SEEK_SET);
		msg[0] = '\0';
		while(!feof(fquery)) {		
			msg[i] = fgetc(fquery);
			i++;
			msg[i] = '\0';
		}
		
		// send queries for each of the child process along with other informations
		for (i = 1; i < n_procs; i++) {
			// send the database first
			MPI_Send(&lines, 1, MPI_LONG, i, STD_TAG, MPI_COMM_WORLD);
			for (int k = 0; k < lines; k++) {
				MPI_Send(bases + k*1000001, 1000001, MPI_CHAR, i, STD_TAG, MPI_COMM_WORLD);
			}

			// then send the queries
			MPI_Send(&fileSize, 1, MPI_LONG, i, STD_TAG, MPI_COMM_WORLD);
			MPI_Send(&queryLines, 1, MPI_LONG, i, STD_TAG, MPI_COMM_WORLD);
			MPI_Send(msg, fileSize, MPI_CHAR, i, STD_TAG, MPI_COMM_WORLD);			
		}

		double parallelTime = MPI_Wtime();

		// main process job
		char desc_query[100];
		
		int j = 0;
		int length = 0;
		i = 0;
		while (j < queryLines/n_procs) {			
			// read the query description
			length = 0;
			while (msg[i] != '\n' && length < 100 && msg[i] != '\0') {
				line[length] = msg[i];
				line[length + 1] = '\0';
				i++;
				length++;				
			}
			

			strcpy(desc_query, line);
			remove_eol(desc_query);

			// read the query dna
			i++;
			str[0] = 0;
			length = 0;
			while (msg[i] != '>' && msg[i] != '\0') {
				if (msg[i] != '\n') {
					str[length] = msg[i];
					str[length + 1] = '\0';		

					length++;
				}
				i++;
			}

			strcat(output, desc_query);
			strcat(output, "\n");

			result = 0;
			found = 0;

			for (int k = 1; k < lines; k += 2) {
				result = bmhs(bases + k*1000001, strlen(bases + k*1000001), str, strlen(str));
				if (result > 0) {
					strcat(output, bases + (k - 1)*1000001);
					strcat(output, "\n");
					sprintf(collum, "%d", result);
					strcat(output, collum);
					strcat(output, "\n");
					found++;
				}
			}

			if (!found) {
				strcat(output, "NOT FOUND\n");
			}			

			j++;
		}
		fprintf(fout, "%s", output);

		// print the results from each of the child process
		for (i = 1; i < n_procs; i++) {
			output[0] = 0;
			MPI_Recv(output, fileSize, MPI_CHAR, i, MPI_ANY_TAG, MPI_COMM_WORLD, &status);					
			fprintf(fout, "%s", output);
		}

		printf("O tempo de execuçao paralela durou: %f\n", MPI_Wtime() - parallelTime);

		closefiles();
	}
	// other process compares others parts of the file
	else {
		// receive the bases
		MPI_Recv(&lines, 1, MPI_LONG, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

		bases = (char*) malloc(lines * 1000001 * sizeof(char));
		if (bases == NULL) {
			perror("malloc");
			exit(EXIT_FAILURE);
		}

		for (int k = 0; k < lines; k++){
			MPI_Recv(bases + k*1000001, 1000001, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		}
		
		// receive the queries
		MPI_Recv(&fileSize, 1, MPI_LONG, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		MPI_Recv(&queryLines, 1, MPI_LONG, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		

		char *msg  = (char*) malloc(sizeof(char) * fileSize);
		if (msg== NULL) {
			perror("malloc msg");
			exit(EXIT_FAILURE);
		}

		// receive the file into memory from the father process
		MPI_Recv(msg, fileSize, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);	

		// output array string for merging the results
		char *output  = (char*) malloc(sizeof(char) * fileSize);
		if (output== NULL) {
			perror("malloc msg");
			exit(EXIT_FAILURE);
		}

		char collum[11];
		output[0] = 0;
		collum[0] = 0;
		// go to the corresponding line for the current process
		int j = 0;
		i = 0;
		for (i = 1; i < fileSize; i++){
			if (msg[i] == '>') {
				j++;
				if (j == my_rank*(queryLines/n_procs)) {
					break;
				}
			}
		}

		// child process job
		char desc_query[100];

		long long int maxProcessing = queryLines/n_procs;
		maxProcessing = (my_rank + 1) * maxProcessing;
		if (my_rank == n_procs - 1) {
			maxProcessing += queryLines%n_procs;
		}

		int length = 0;
		while (j < maxProcessing) {			
			// read the query description
			length = 0;
			while (msg[i] != '\n' && length < 100 && msg[i] != '\0') {
				line[length] = msg[i];
				line[length + 1] = '\0';
				i++;
				length++;				
			}
			

			strcpy(desc_query, line);
			remove_eol(desc_query);
			
			// read the query dna
			i++;
			str[0] = 0;
			length = 0;
			while (msg[i] != '>' && msg[i] != '\0') {
				if (msg[i] != '\n') {
					str[length] = msg[i];
					str[length + 1] = '\0';		

					length++;
				}
				i++;
			}

			strcat(output, desc_query);
			strcat(output, "\n");

			result = 0;
			found = 0;

			for (int k = 1; k < lines; k += 2) {
				result = bmhs(bases + k*1000001, strlen(bases + k*1000001), str, strlen(str));
				if (result > 0) {
					strcat(output, bases + (k - 1)*1000001);
					strcat(output, "\n");
					sprintf(collum, "%d", result);
					strcat(output, collum);
					strcat(output, "\n");
					found++;
				}
			}

			if (!found) {
				strcat(output, "NOT FOUND\n");
			}			

			j++;
		}

		// return the processed piece to the father process
		MPI_Send(output, fileSize, MPI_CHAR, 0, STD_TAG, MPI_COMM_WORLD);
	}

	free(str);
	free(bases);	
	MPI_Finalize();

	if (my_rank == 0) {
		printf("O programa executou em: %f\n", MPI_Wtime() - start);
	}

	return EXIT_SUCCESS;
}
