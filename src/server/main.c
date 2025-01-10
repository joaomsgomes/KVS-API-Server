#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>
#include <sys/stat.h>
#include <signal.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include "pthread.h"
#include "../common/protocol.h"

struct SharedData {
  DIR* dir;
  char* dir_name;
  pthread_mutex_t directory_mutex;
};

pthread_mutex_t sessions_lock = PTHREAD_MUTEX_INITIALIZER; 
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_sessions = 0;
size_t active_backups = 0;     // Number of active backups
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads
char* jobs_directory = NULL;

int req_pipes[S];
int res_pipes[S];
int notif_pipes[S];

char res_output[MAX_JOB_FILE_NAME_SIZE];

char output[MAX_STRING_SIZE];

int start_session() {

    pthread_mutex_lock(&sessions_lock);

    if (active_sessions >= S) {  
        pthread_mutex_unlock(&sessions_lock);  
        return -1;  
    }

    active_sessions++;  

    pthread_mutex_unlock(&sessions_lock);  
    
    return 0;
    
  }

void close_cleanPipes(int position) {

  close(notif_pipes[position]);
  close(res_pipes[position]);
  close(req_pipes[position]);

  notif_pipes[position] = '\0';
  res_pipes[position] = '\0';
  req_pipes[position] = '\0';

}


int filter_job_files(const struct dirent* entry) {
    const char* dot = strrchr(entry->d_name, '.');
    if (dot != NULL && strcmp(dot, ".job") == 0) {
        return 1;  // Keep this file (it has the .job extension)
    }
    return 0;
}

static int entry_files(const char* dir, struct dirent* entry, char* in_path, char* out_path) {
  const char* dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 || strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char* filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          write_str(STDERR_FILENO, "Failed to write pair\n");
        }

        break;

      case CMD_READ:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(out_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(in_fd, &delay, NULL) == -1) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          printf("Waiting %d seconds\n", delay / 1000);
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        pthread_mutex_lock(&n_current_backups_lock);
        if (active_backups >= max_backups) {
          wait(NULL);
        } else {
          active_backups++;
        }
        pthread_mutex_unlock(&n_current_backups_lock);
        int aux = kvs_backup(++file_backups, filename, jobs_directory);

        if (aux < 0) {
            write_str(STDERR_FILENO, "Failed to do backup\n");
        } else if (aux == 1) {
          return 1;
        }
        break;

      case CMD_INVALID:
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        write_str(STDOUT_FILENO,
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n");

        break;

      case CMD_EMPTY:
        break;

      case EOC:
        printf("EOF\n");
        return 0;
    }
  }
}

//frees arguments
static void* get_file(void* arguments) {
  struct SharedData* thread_data = (struct SharedData*) arguments;
  DIR* dir = thread_data->dir;
  char* dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent* entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}


static void dispatch_threads(DIR* dir) {
  pthread_t* threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};


  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  // ler do FIFO de registo

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}

int get_client_slot() {
  for (int i = 0; i < S ; i++) {
    if (req_pipes[i] == '\0') {
      return i;
    }
  }
  return -1;
}

void sigusr1_handler() {
    // Bloquear o acesso a active_sessions enquanto modifica
    pthread_mutex_lock(&sessions_lock);
    
    if (active_sessions > 0) {
        // Encontra uma sessão ativa para encerrar
        for (int i = 0; i < S; i++) {
            if (req_pipes[i] != '\0') { // Verifica se o pipe está ativo
                fprintf(stderr, "Interrupting session for client at position %d\n", i);
                close_cleanPipes(i); // Fecha e limpa os pipes
                active_sessions--;
                break; // Sai após encerrar uma sessão
            }
        }
    }

    pthread_mutex_unlock(&sessions_lock);
}


int handle_client_subscription(char* buffer , ssize_t req_bytes_read, int position) {

  if (req_bytes_read < 0) {
    fprintf(stderr, "Invalid message size.\n");
    return -1;
  }

    buffer[req_bytes_read] = '\0'; // Garantir terminação nula

    // Separar os campos do buffer
  char* token = strtok(buffer, "|");
  
  if (token == NULL) {
    fprintf(stderr, "Invalid message format (no OP_CODE).\n");
    return -1;
  }

  char* key = strtok(NULL, "|");

  if (key == NULL) {
        fprintf(stderr, "Invalid message format (missing KEY).\n");
        return -1;
  }

  int notif_id = notif_pipes[position];
  
  return add_key_subscriber(key, notif_id);

}

int handle_client_unsubscription(char* buffer, ssize_t req_bytes_read, int position) {
  
  if (req_bytes_read < 0) {
    fprintf(stderr, "Invalid message size.\n");
    return -1;
  }

  buffer[req_bytes_read] = '\0'; // Garantir terminação nula

    // Separar os campos do buffer
  char* token = strtok(buffer, "|");
  
  if (token == NULL) {
    fprintf(stderr, "Invalid message format (no OP_CODE).\n");
    return -1;
  }

  char* key = strtok(NULL, "|");

  if (key == NULL) {
        fprintf(stderr, "Invalid message format (missing KEY).\n");
        return -1;
  }

  int notif_id = notif_pipes[position];
  
  return remove_key_subscriber(key, notif_id);
}

int handle_client_unregister(int position) {

 int notif_pipes_id  = notif_pipes[position];

 return disconnect_client(notif_pipes_id);
  
}

int handle_client_register(char* buffer, ssize_t reg_bytes_read) {

    if (reg_bytes_read < 0) {
        fprintf(stderr, "Invalid message size.\n");
        return -1;
    }

    // 1|pipe_pedido|pipe_resposta|pipe_notif

    buffer[reg_bytes_read] = '\0'; // Garantir terminação nula

    // Separar os campos do buffer
    char* token = strtok(buffer, "|");
    if (token == NULL) {
        fprintf(stderr, "Invalid message format (no OP_CODE).\n");
        return -1;
    }

    char op_code = token[0];
    printf("OP_CODE: %c\n", op_code);

    // Extrair nomes dos FIFOs
    char* fifo_requests = strtok(NULL, "|");
    char* fifo_responses = strtok(NULL, "|");
    char* fifo_notifications = strtok(NULL, "|");

    if (fifo_requests == NULL || fifo_responses == NULL || fifo_notifications == NULL) {
        fprintf(stderr, "Invalid message format (missing FIFO names).\n");
        return -1;
    }

    // Abrir FIFOs do cliente
    int requests_fd = open(fifo_requests, O_RDONLY);
    if (requests_fd == -1) {
        perror("Failed to open requests FIFO");
        return -1;
    }

    int responses_fd = open(fifo_responses, O_WRONLY);
    
    if (responses_fd == -1) {
        perror("Failed to open responses FIFO");
        close(requests_fd);
        return -1;
    }

    int notifications_fd = open(fifo_notifications, O_WRONLY);
    if (notifications_fd == -1) {
        perror("Failed to open notifications FIFO");
        close(requests_fd);
        return -1;
    }

    int position = get_client_slot();
    
    // Adds pipes to server
    req_pipes[position] = requests_fd; 
    res_pipes[position] = responses_fd;
    notif_pipes[position] = notifications_fd;
    
    // FIFOs opened successfully
    return responses_fd;
}

int main(int argc, char** argv) {

  struct sigaction sa;
  sa.sa_handler = sigusr1_handler;
  sa.sa_flags = 0;
  sigemptyset(&sa.sa_mask);
  

  if (sigaction(SIGUSR1, &sa, NULL) == -1) {
        perror("Failed to set SIGUSR1 handler");
        exit(1);
  }

  if (argc < 5) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
		write_str(STDERR_FILENO, " <max_threads>");
		write_str(STDERR_FILENO, " <max_backups> \n");
    write_str(STDERR_FILENO, " <register_pipe_path> \n");
    return 1;
  }

  jobs_directory = argv[1];

  char* endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

	if (max_backups <= 0) {
		write_str(STDERR_FILENO, "Invalid number of backups\n");
		return 0;
	}

	if (max_threads <= 0) {
		write_str(STDERR_FILENO, "Invalid number of threads\n");
		return 0;
	}

  const char* register_pipe = argv[4];

  if (mkfifo(register_pipe, 0666) == -1) {
    write_str(STDERR_FILENO, "Failed to create FIFO\n");
    return 1;
  }

  write_str(STDOUT_FILENO, "criadoo");
  
  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  write_str(STDOUT_FILENO, "Directory opened: \n");

  // Abrir FIFO para leitura
  snprintf(output, MAX_WRITE_SIZE, "Register pipe path: %s\n", register_pipe);
  write_str(STDOUT_FILENO, output);
  int reg_pipe_fd = open(register_pipe, O_RDONLY);
  
  if (reg_pipe_fd == -1) {
      perror("Failed to open FIFO for reading");
      closedir(dir);
      return 1;
  }
  write_str(STDOUT_FILENO, "Pipe opened: \n");
  write_str(STDOUT_FILENO, "ola\n");
  // Monitorar mensagens no FIFO
  char buffer[MAX_WRITE_SIZE*3 + 1];
  while (1) {
    write_str(STDOUT_FILENO, "reading pipe...not cooking\n");
      ssize_t reg_bytes_read = read(reg_pipe_fd, buffer, sizeof(buffer) - 1);

      if (reg_bytes_read > 0) {
        
          buffer[reg_bytes_read] = '\0';

          int res_pipe;
          if ((res_pipe = handle_client_register(buffer, reg_bytes_read)) == -1) {
            perror("Error handling client regist\n");
            continue;
          }
          start_session(); //TEMPORARIO
          write_str(res_pipe  , "1|0\n");
          close(res_pipe);
          
      } else if (reg_bytes_read == 0) {
          sleep(5);
          printf("No clients connected. Waiting...\n");
      } else {
          perror("Error reading from FIFO\n");
          break;
      }

      if (active_sessions > 0) {
        for (size_t r = 0; r < S; r++) {
          ssize_t req_bytes_read = read(req_pipes[r], buffer, sizeof(buffer) - 1);

          if (req_bytes_read > 0) {
            int op_code = buffer[0];
            int result;
            
            switch (op_code) {
              
              case OP_CODE_DISCONNECT:
                
                result = handle_client_unregister((int)r);
                
                if (result != -1) {
                  snprintf(res_output, sizeof(res_output), "2|%d", result);
                  write_str(res_pipes[r], res_output);
                }
                close_cleanPipes((int) r);

                break;

              case OP_CODE_SUBSCRIBE:
                
                result = handle_client_subscription(buffer, req_bytes_read, (int) r);
                if (result != -1) {
                  snprintf(res_output, sizeof(res_output), "3|%d", result);
                  write_str(res_pipes[r], res_output);
                }
                break;

              case OP_CODE_UNSUBSCRIBE:
                
                result = handle_client_unsubscription(buffer, req_bytes_read, (int) r);
                if (result != -1) {
                  snprintf(res_output, sizeof(res_output), "4|%d", result);
                  write_str(res_pipes[r], res_output);
                }
                
                break;

              default:
                break;
            }
          }
          
        }
      }

        

  }

  close(reg_pipe_fd);
  unlink(register_pipe); // Remover o FIFO

  dispatch_threads(dir);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();

  return 0;
}
