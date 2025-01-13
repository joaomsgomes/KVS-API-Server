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

pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;


void thread_safe_print(const char* message) {
    pthread_mutex_lock(&print_mutex);
    write(STDOUT_FILENO,message, strlen(message));
    pthread_mutex_unlock(&print_mutex);
}

void end_client() {
  printf("ENDING CLIENT\n");
  pthread_mutex_destroy(&print_mutex);
  pthread_cancel(notif_tid);
  pthread_join(notif_tid, NULL);
  close(req_pipe_fd);
  close(resp_pipe_fd);
  close(notif_pipe_fd);
}

void* notification_thread(void* arg) {
    const char* notif_pipe_path = (const char*)arg;

    //printf("Notification path (client notif thread): %s\n", notif_pipe_path);

    notif_pipe_fd = open(notif_pipe_path, O_RDONLY);
    if (notif_pipe_fd == -1) {
      perror("Error Opening notification FIFO\n");
      free(arg);
      end_client();
      return NULL;
    }
    
    char buffer[MAX_WRITE_SIZE];

    while (1) {
        ssize_t bytes_read = read(notif_pipe_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read > 0) {
          buffer[bytes_read] = '\0'; // Garantir que o buffer seja uma string válida

          if (strcmp("SIGUSR1", buffer) == 0) {
            printf("SIGUS RECEIVED -> aborting client\n");
            break;
          }
          
          // Percorrer o buffer para adicionar espaçamento entre os pares
          char spaced_buffer[2 * sizeof(buffer)]; // Buffer maior para incluir novas linhas
          size_t j = 0; // Índice para o spaced_buffer

          for (size_t i = 0; i < (size_t)bytes_read; i++) {
              spaced_buffer[j++] = buffer[i];

              // Adicionar uma nova linha após cada ')'
              if (buffer[i] == ')') {
                  spaced_buffer[j++] = '\n';
              }
          }

          // Garantir que o spaced_buffer seja uma string válida
          spaced_buffer[j] = '\0';

          // Escrever o buffer com espaçamento no STDOUT
          write_all(STDOUT_FILENO, spaced_buffer, j);
        } else if (bytes_read == 0) {
            // Fim do FIFO
            break;
        } else {
            perror("Erro ao ler do FIFO de notificações");
            end_client();
            break;
        }
    }

    free(arg);
    //sleep(5)
    exit(1);
    //return NULL;
}

int print_output(const char* operation) {

  char response[MAX_WRITE_SIZE];
  char output[MAX_WRITE_SIZE];

  ssize_t bytes_read = read(resp_pipe_fd, response, MAX_WRITE_SIZE);

  if (bytes_read <= 0) {
    perror("Failed to read response pipe\n");
    return 1;
  }
  response[bytes_read] = '\0';

  
  snprintf(output, sizeof(output), "Server returned %c for operation: %s\n", response[bytes_read - 2], operation);
  thread_safe_print(output);

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

  printf("Preparing to open server pipe\n");

  int server_fd = open(server_pipe_path, O_WRONLY);

  if (server_fd == -1) {
    perror("Error Opening Register FIFO\n");
    exit(EXIT_FAILURE);
  }

  printf("Successfulyy opened server pipe\n");

  snprintf(request, sizeof(request), "1|%s|%s|%s", req_pipe_path, resp_pipe_path, notif_pipe_path);
  
  printf("Requesting: %s\n", request);

  if (write_all(server_fd, request, strlen(request)) == -1) {
    perror("Error Registering in Server\n");
    return -1;
  }

  printf("Request sent successfully\n");

  req_pipe_fd = open(req_pipe_path, O_WRONLY);
    if (req_pipe_fd < 0) {
        perror("Requests: Error opening requests' FIFO");
        close(notif_pipe_fd);
        return 1;
    }

  
  resp_pipe_fd = open(resp_pipe_path, O_RDONLY);
  if (resp_pipe_fd == -1) {
      perror("Respostas: Error opening Responses' FIFO");
      close(req_pipe_fd);
      close(notif_pipe_fd);
      return 1;
  }
    
  if (print_output("connect")) {
    end_client();
    return -1;
  }

  return 0;
}
 
int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  
  if (write_all(req_pipe_fd, "2", 2) == -1) {
    perror("Error Unregistering from Server\n");
    end_client();
    return -1;
  }
  
  if (print_output("disconnect")) {
    perror("Error Sending Disconnect Message\n");
    end_client();
    return -1;
  }

  end_client();

  return 0;
}



int kvs_subscribe(const char* key) {
  
  snprintf(request, sizeof(request) + 2, "3|%s", key);

  if (write_all(req_pipe_fd, request, strlen(key) + 3) == -1) {
    perror("Error Subscribing Key in Server\n");
    end_client();
    return 1;
  }

  if (print_output("subscribe")) {
    end_client();
    return 1;
  }

  return 0;
}

int kvs_unsubscribe(const char* key) {
  
  snprintf(request, sizeof(request) + 2, "4|%s", key);

  if (write_all(req_pipe_fd, request, strlen(key) + 3) == -1) {
    perror("Error Unsubscribing Key in Server\n");
    end_client();
    return 1;
  }

  if (print_output("unsubscribe")) {
    end_client();
    return 1;
  }
  
  return 0;
}


