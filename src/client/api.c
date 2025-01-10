#include "api.h"
#include "src/common/io.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

char* req_pipe;
char* res_pipe;
char* notif_pipe;

char request[MAX_STRING_SIZE];

int print_output(const char* operation) {

  char response[MAX_STRING_SIZE];
  char output[MAX_WRITE_SIZE];

  int response_fd = open(res_pipe, O_RDONLY);
  if (response_fd < 0) {
    perror("Failed to open response pipe");
    return 1;
  }

  ssize_t bytes_read = read(response_fd, response, strlen(response) - 1);

  if (bytes_read == 0) {
    perror("Failed to read response pipe\n");
    close(response_fd);
    return 1;
  }

  response[bytes_read] = '\0';
  snprintf(output, sizeof(output), "Server returned %d for operation: %s\n", response[bytes_read - 1], operation);

  if (write(STDOUT_FILENO, output, strlen(output)) < 0) {
    perror("Failed to write to stdout");
    close(response_fd);
    return 1;
  }

  close(response_fd);
  return 0;

  return 0;
}

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* notif_pipe_path,
                char const* server_pipe_path) {

  if (mkfifo(req_pipe_path, 0666) == -1) {
    write(STDERR_FILENO, "Failed to create request FIFO\n", 31);
    return -1;
  }
  
  if (mkfifo(resp_pipe_path, 0666) == -1) {
    write(STDERR_FILENO, "Failed to create response FIFO\n", 32);
    return -1;
  }
  
  if (mkfifo(notif_pipe_path, 0666) == -1) {
    write(STDERR_FILENO, "Failed to create notifications FIFO\n", 37);
    return -1;
  }

  strcpy(req_pipe, req_pipe_path);
  strcpy(res_pipe, resp_pipe_path);
  strcpy(notif_pipe, notif_pipe_path);

  int server_fd = open(server_pipe_path, O_WRONLY);

  if (server_fd == -1) {
    perror("Error Opening Register FIFO\n");
    exit(EXIT_FAILURE);
  }

  snprintf(request, sizeof(request), "%s|%s|%s\n", req_pipe_path, resp_pipe_path, notif_pipe_path);


  if (write_all(server_fd, request, strlen(request)) == -1) {
    perror("Error Registing in Server\n");
    return -1;
  }

  if (print_output("connect")) {
    return -1;
  }

  return 0;
}
 
int kvs_disconnect(void) {
  // close pipes and unlink pipe files

  int req_fd = open(req_pipe, O_WRONLY);

  if (write_all(req_fd, "2", 1) == -1) {
    perror("Error Registering in Server\n");
    close(req_fd);
    return -1;
  }
  close(req_fd);

  if (print_output("disconnect")) {
    return -1;
  }

  return 0;

}

int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe

  int req_fd = open(req_pipe, O_WRONLY);
  
  snprintf(request, sizeof(request) + 2, "3|%s", key);

  if (write_all(req_fd, request, strlen(request)) == -1) {
    perror("Error Registering in Server\n");
    close(req_fd);
    return 1;
  }
  close(req_fd);

  if (print_output("subscribe")) {
    return 1;
  }

  return 0;
}

int kvs_unsubscribe(const char* key) {
  // send unsubscribe message to request pipe and wait for response in response pipe

  int req_fd = open(req_pipe, O_WRONLY);

  snprintf(request, sizeof(request) + 2, "4|%s", key);

  if (write_all(req_fd, request, strlen(request)) == -1) {
    perror("Error Registering in Server\n");
    close(req_fd);
    return 1;
  }
  close(req_fd);

  if (print_output("unsubscribe")) {
    return 1;
  }


  return 0;
}


