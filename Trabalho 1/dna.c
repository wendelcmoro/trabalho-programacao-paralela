#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>

// MAX char table (ASCII)
#define MAX 256

// Number of Threads to be used
#define THREADS 10

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

int main(void) {

	double startTime = omp_get_wtime();
	double seqTimeInit = omp_get_wtime();
	double sequencialTime = 0;
	double parallelTime = 0;

	omp_set_num_threads(THREADS);

	str = (char*) malloc(sizeof(char) * THREADS * 1000001);
	if (str == NULL) {
		perror("malloc str");
		exit(EXIT_FAILURE);
	}

	openfiles();

	int ch = 0;
	long long int lines = 0;

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

	char line[100];
	int iLine = 0;
	int offset = 0;
	int i, found, result;

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

	char desc_query[THREADS][100];
	offset = 0;
	i = 0;
	iLine = 0;
	
	fgets(line, 100, fquery);
	remove_eol(line);
	
	double palTimeInit = 0;
	double palTimeEnd = 0;
	double seqTimeFinish = omp_get_wtime();
	sequencialTime = seqTimeFinish - seqTimeInit;
	seqTimeInit = omp_get_wtime();
	while (!feof(fquery)) {		
		strcpy(desc_query[iLine], line);

		// read query string
		str[offset] = 0;
		i = 0;
		fgets(line, 100, fquery);
		remove_eol(line);	
		do {
			strcat(str + offset + i, line);
			if (fgets(line, 100, fquery) == NULL)
				break;
			remove_eol(line);
			i += strlen(line);
		} while (line[0] != '>');
		iLine++;

		if (iLine == THREADS) {
			int j;
			int k;

			seqTimeFinish = omp_get_wtime();
			sequencialTime += seqTimeFinish - seqTimeInit;
			palTimeInit = omp_get_wtime();			
			#pragma omp parallel for ordered collapse(1) private(j, k, result, found)
			for (k = 0; k < iLine; k++) {
				char output[1000001];				
				char collum[11];	
				output[0] = 0;
				collum[0] = 0;					
				strcat(output, desc_query[k]);
				strcat(output, "\n");
				found = 0;
				for (j = 1; j < lines; j += 2) {
					result = bmhs(bases + j*1000001, strlen(bases + j*1000001), str + k*1000001, strlen(str + k*1000001));
					if (result > 0) {						
						found++;
						strcat(output, bases + (j - 1)*1000001);
						strcat(output, "\n");
						sprintf(collum, "%d", result);
						strcat(output, collum);
						strcat(output, "\n");
					}
				}
				#pragma omp ordered
				if (found == 0) {
					strcat(output, "NOT FOUND\n");
					fprintf(fout, "%s", output);
				}
				else {
					fprintf(fout, "%s", output);
				}
			}
			palTimeEnd = omp_get_wtime();
			parallelTime += palTimeEnd - palTimeInit;
			seqTimeInit = omp_get_wtime();

			iLine = 0;
		}
		offset = iLine * 1000001;

		seqTimeFinish = omp_get_wtime();
		sequencialTime += seqTimeFinish - seqTimeInit;

		seqTimeInit = omp_get_wtime();
	}
	// compare the remaining iLines in parallel
	int j;
	int k;

	seqTimeFinish = omp_get_wtime();
	sequencialTime += seqTimeFinish - seqTimeInit;
	palTimeInit = omp_get_wtime();
	#pragma omp parallel for ordered collapse(1) private(j, k, result, found,  i)	
	for (k = 0; k < iLine; k++) {
		char output[1000001];
		char collum[11];
		collum[0] = 0;	
		output[0] = 0;
		strcat(output, desc_query[k]);
		strcat(output, "\n");

		found = 0;
		for (j = 1; j < lines; j += 2) {
			result = bmhs(bases + j*1000001, strlen(bases + j*1000001), str + k*1000001, strlen(str + k*1000001));
			if (result > 0) {							
				found++;
				strcat(output, bases + (j - 1)*1000001);
				strcat(output, "\n");
				sprintf(collum, "%d", result);
				strcat(output, collum);
				strcat(output, "\n");
			}
		}
		#pragma omp ordered
		if (found == 0) {
			strcat(output, "NOT FOUND\n");
			fprintf(fout, "%s", output);
		}
		else {
			fprintf(fout, "%s", output);
		}
	}
	palTimeEnd = omp_get_wtime();
	parallelTime += palTimeEnd - palTimeInit;

	seqTimeInit = omp_get_wtime();
	
	closefiles();

	free(str);
	free(bases);

	seqTimeFinish = omp_get_wtime();
	sequencialTime += seqTimeFinish - seqTimeInit;

	double endTime = omp_get_wtime();

    printf ("\nO tempo de execução total durou %f\nO tempo de execução sequencial durou %f\nO tempo de execução paralela durou %f\n", endTime - startTime, sequencialTime, parallelTime);
	return EXIT_SUCCESS;
}
