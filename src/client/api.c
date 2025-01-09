#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* notif_pipe_path,
                char const* server_pipe_path) {

  if (mkfifo(req_pipe_path) <= 0) {
    write_str(STDERR_FILENO, "Failed to create request FIFO\n");
    return -1;
  }
  
  if (mkfifo(resp_pipe_path) <= 0) {
    write_str(STDERR_FILENO, "Failed to create response FIFO\n");
    return -1;
  }
  
  if (mkfifo(notif_pipe_path) <= 0) {
    write_str(STDERR_FILENO, "Failed to create notifications FIFO\n");
    return -1;
  }

  int server_fd = open(server_pipe_path, O_WRONLY);

  if (server_fd == -1) {
    perror("Error Opening Register FIFO\n");
    exit(EXIT_FAILURE);
  }

  char message[128];
  snprintf(message, sizeof(message), "%s|%s|%s\n", req_pipe_path, resp_pipe_path, notif_pipe_path);

  if (write(server_fd, message, strlen(message)) == -1) {
    perror("Error Registing in Server\n");
    return -1;
  }

  int response_fd = open(resp_pipe_path, O_RDONLY);
  char *output = read(response_fd, message, strlen(message) - 1);
  write_str(stdout, "Server returned 0 for operation: connect\n");

  return 0;
}
 
int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  return 0;
}

int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
  return 0;
}

int kvs_unsubscribe(const char* key) {
    // send unsubscribe message to request pipe and wait for response in response pipe
  return 0;
}


