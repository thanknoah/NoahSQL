// __WARNING__: THIS IS NOT PART OF NOAHSQL, THIS IS STILL WORK IN PRGORESS
#include <stdio.h>
#include <stdlib.h>

void deleteTable(const char *FILE_NAME, char STR_TO_DELETE[]) {
  FILE *DELETE_FROM_CURRENT_FILE = fopen(FILE_NAME, "r");
  FILE *TEMP_FILE = fopen("temp.txt", "w");

  if (DELETE_FROM_CURRENT_FILE == NULL || TEMP_FILE == NULL) {
    printf("SYS: Table does not exist, or lack administration permissions");
  } else {
    char LINE[256];

    while (fgets(LINE, sizeof(LINE), DELETE_FROM_CURRENT_FILE) != NULL) {
      if (strstr(LINE, STR_TO_DELETE) == NULL) {
        fputs(LINE, TEMP_FILE);
        printf(LINE);
        printf(STR_TO_DELETE);
      }
    }
  }

  fclose(DELETE_FROM_CURRENT_FILE);
  remove(FILE_NAME);
  rename("temp.txt", FILE_NAME);
}

void writeTable(const char *FILE_NAME, const char *CHAR_TO_WRITE) {
  FILE *TABLE_WRITE;
  TABLE_WRITE = fopen(FILE_NAME, "a");

  if (TABLE_WRITE == NULL) {
    printf("SYS: Table does not exist");
  } else {
    fprintf(TABLE_WRITE, "%s\n", CHAR_TO_WRITE);
  }

  fclose(TABLE_WRITE);
}

void readTable(const char FILE_NAME[]) {
  FILE *TABLE_READ;
  TABLE_READ = fopen(FILE_NAME, "r");
  char LINE[256];

  if (TABLE_READ == NULL) {
    printf("SYS: Table does not exist");
  } else {
    while (fgets(LINE, sizeof(LINE), TABLE_READ) != NULL) {
      printf("%s", LINE);
    }
  }

  fclose(TABLE_READ);
}

int main() {
  // readTable("random.json");
  // writeTable("random.json", "hello world");
  // deleteTable("random.json", "hello world");
  return 0;
}
