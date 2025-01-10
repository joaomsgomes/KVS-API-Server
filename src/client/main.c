#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"


void* notification_thread(void* arg) {
    int notif_fd = *(int*)arg;
    char buffer[256];

    while (1) {
        ssize_t bytes_read = read(notif_fd, buffer, sizeof(buffer) - 1);
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

    return NULL;
}

void* request_thread(void* arg) {
    
    int req_fd = *(int*)arg;

    while (1) {
        const char* request = "Simulando pedido ao servidor\n";
        if (write(req_fd, request, strlen(request)) < 0) {
            perror("Erro ao escrever no FIFO de pedidos");
            break;
        }
        sleep(5); 
    }

    return NULL;
}

int main(int argc, char* argv[]) {
  
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
    return 1;
  }

  char req_pipe_path[256] = "/tmp/req";
  char resp_pipe_path[256] = "/tmp/resp";
  char notif_pipe_path[256] = "/tmp/notif";

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char)); // Adding Client's ID to each Pipe
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  const char* server_pipe_path = argv[2];
                
  if (kvs_connect(req_pipe_path, resp_pipe_path, notif_pipe_path, server_pipe_path) != 0) {
    
    write_all(STDERR_FILENO, "Failed to connect to the server\n", 33);
    write_all(STDOUT_FILENO, "Server returned 0 for operation: connect\n", 42);
    return 1;
  }

  int notif_fd = open(notif_pipe_path, O_RDONLY);
  if (notif_fd < 0) {
        perror("Erro ao abrir FIFO de notificações");
        return 1;
    }

  int req_fd = open(req_pipe_path, O_WRONLY);
  if (req_fd < 0) {
        perror("Erro ao abrir FIFO de pedidos");
        close(notif_fd);
        return 1;
  } 

  pthread_t notif_tid, req_tid;

  pthread_create(&notif_tid, NULL, notification_thread, &notif_fd);
  pthread_create(&req_tid, NULL, request_thread, &req_fd);

  while (1) {
    switch (get_next(STDIN_FILENO)) {
      case CMD_DISCONNECT:
        if (kvs_disconnect() != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          return 1;
        }
        pthread_join(notif_tid, NULL);
        // TODO: end notifications thread
        printf("Disconnected from server\n");
        return 0;

      case CMD_SUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
         
        if (kvs_subscribe(keys[0])) {
            fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_UNSUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
         
        if (kvs_unsubscribe(keys[0])) {
            fprintf(stderr, "Command subscribe failed\n");
        }

        break;
        

      case CMD_DELAY:
        if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay_ms > 0) {
            printf("Waiting...\n");
            delay(delay_ms);
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_EMPTY:
        break;

      case EOC:
        // input should end in a disconnect, or it will loop here forever
        if (kvs_disconnect() != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          return 1;
        }
        
        printf("Disconnected from server\n");
        return 0;
    }
  }
}
