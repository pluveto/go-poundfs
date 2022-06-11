
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
  // Create "/tmp/mp/new.txt" and write "Hello, World!" to it.
  FILE* fp = fopen("/tmp/mp/new.txt", "w");
  if (fp == NULL) {
    fprintf(stderr, "Error opening file: %s", strerror(errno));
    exit(1);
  }
  printf("fp: %p\n", fp);
  fprintf(fp, "Hello, World!");
  int ret;
  ret = fclose(fp);
  if (ret != 0) {
    fprintf(stderr, "Error closing file: %s", strerror(errno));
    exit(1);
  }
  // Read file "/tmp/mp/myfile.txt" and print the content.
  fp = fopen("/tmp/mp/new.txt", "r");
  if (fp == NULL) {
    printf("Error opening file!\n");
    exit(1);
  }
  // read until end of file
  char buf[64];
  while (fgets(buf, 64, fp) != NULL) {
    printf("%s", buf);
  }
  ret = fclose(fp);
  if (ret != 0) {
    fprintf(stderr, "Error closing file: %s", strerror(errno));
    exit(1);
  }
  return 0;
}
