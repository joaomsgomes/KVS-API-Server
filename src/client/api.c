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

int req_pipe_fd;
int resp_pipe_fd;
int notif_pipe_fd;

pthread_t notif_tid;

char request[MAX_STRING_SIZE*3+5];

void* notification_thread(void* arg) {
    const char* notif_pipe_path = (const char*)arg;

    printf("Notification path (client notif thread): %s\n", notif_pipe_path);

    notif_pipe_fd = open(notif_pipe_path, O_RDONLY);
    if (notif_pipe_fd == -1) {
      perror("thread_notif: Erro ao abrir FIFO de notificações");
      free(arg);
      return NULL;
    }

    printf("Notif pipe opened\n");

    char buffer[256];

    while (1) {
        ssize_t bytes_read = read(notif_pipe_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read > 0) {
            buffer[bytes_read] = '\0'; // Garantir que é uma string válida
            printf("Notification: %s\n", buffer);
        } else if (bytes_read == 0) {
            // Fim do FIFO
            break;
        } else {
            perror("Erro ao ler do FIFO de notificações");
            break;
        }
    }

    free(arg);
    return NULL;
}

int print_output(const char* operation) {

  char response[MAX_WRITE_SIZE];
  char output[MAX_WRITE_SIZE];

  ssize_t bytes_read = read(resp_pipe_fd, response, MAX_WRITE_SIZE);

  snprintf(output, MAX_WRITE_SIZE, "Bytes Read: %ld\n", bytes_read);
  write(STDOUT_FILENO, output, MAX_WRITE_SIZE);

  if (bytes_read < 0) {
    perror("Failed to read response pipe\n");
    return 1;
  }
  response[bytes_read] = '\0';

  snprintf(output, MAX_WRITE_SIZE*4, "Response to client: %s\n", response);
  write(STDOUT_FILENO, output, MAX_WRITE_SIZE);


  if (response[0] != '1') {
    perror("Wrong Response OPCODE");
    return 1;
  }
  snprintf(output, sizeof(output),
  "Server returned %c for operation: %s\n", response[bytes_read - 1], operation);
    

  if (write(STDOUT_FILENO, output, sizeof(output)-1) < 0) {
    perror("Failed to write to stdout");
    
    return 1;
  }

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

  char* notif_pipe_path_copy = strdup(notif_pipe_path);
  pthread_create(&notif_tid, NULL, notification_thread, notif_pipe_path_copy);

  int server_fd = open(server_pipe_path, O_WRONLY);

  if (server_fd == -1) {
    perror("Error Opening Register FIFO\n");
    exit(EXIT_FAILURE);
  }

  snprintf(request, sizeof(request), "1|%s|%s|%s", req_pipe_path, resp_pipe_path, notif_pipe_path);
  
  if (write_all(server_fd, request, strlen(request)) == -1) {
    perror("Error Registering in Server\n");
    return -1;
  }

  req_pipe_fd = open(req_pipe_path, O_WRONLY);
    if (req_pipe_fd < 0) {
        perror("Pedidos: Erro ao abrir FIFO de pedidos");
        close(notif_pipe_fd);
        return 1;
    }

  write_all(STDOUT_FILENO, "Request pipe opened\n", MAX_STRING_SIZE);

  resp_pipe_fd = open(resp_pipe_path, O_RDONLY);
  if (resp_pipe_fd == -1) {
      perror("Respostas: Erro ao abrir FIFO de respostas");
      close(req_pipe_fd);
      close(notif_pipe_fd);
      return 1;
  }

  write_all(STDOUT_FILENO, "Response pipe opened\n", MAX_STRING_SIZE);

  
  if (print_output("connect")) {
    close(resp_pipe_fd);
    return -1;
  }
  

  return 0;
}
 
int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  printf("OLA SEMEDO\n");
  if (write_all(req_pipe_fd, "2", 1) == -1) {
    perror("WTF: Error Unregistering from Server\n");
    close(req_pipe_fd);
    return -1;
  }
  pthread_cancel(notif_tid);
  pthread_join(notif_tid, NULL);

  close(req_pipe_fd);
  close(resp_pipe_fd);
  close(notif_pipe_fd);

  if (print_output("disconnect")) {
    return -1;
  }

  return 0;

}



int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
  
  snprintf(request, sizeof(request) + 2, "3|%s", key);

  if (write_all(req_pipe_fd, request, strlen(request)) == -1) {
    perror("Error Subscribing Key in Server\n");

    return 1;
  }

  if (print_output("subscribe")) {
    return 1;
  }

  return 0;
}

int kvs_unsubscribe(const char* key) {
  // send unsubscribe message to request pipe and wait for response in response pipe


  snprintf(request, sizeof(request) + 2, "4|%s", key);

  if (write_all(req_pipe_fd, request, strlen(request)) == -1) {
    perror("Error Unsubscribing Key in Server\n");
    return 1;
  }

  /*if (print_output("unsubscribe")) {
    return 1;
  }
  */

  return 0;
}


