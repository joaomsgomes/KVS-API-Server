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
#include <semaphore.h>


#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include "pthread.h"
#include "../common/protocol.h"
#include "../common/constants.h"
//#include "semedo.joao"

struct SharedData {
  DIR* dir;
  char* dir_name;
  pthread_mutex_t directory_mutex;
};

pthread_mutex_t buffer_mutex = PTHREAD_MUTEX_INITIALIZER;

sem_t sessions_sem, buffer_sem;

pthread_mutex_t sus_lock = PTHREAD_MUTEX_INITIALIZER; 
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0;     // Number of active backups
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads

pthread_t client_threads[MAX_SESSION_COUNT];

volatile sig_atomic_t received_sigusr1 = 0;

int server_on;
int active_sessions = 0;

char* jobs_directory = NULL;

int req_pipes[MAX_SESSION_COUNT];
int res_pipes[MAX_SESSION_COUNT];
int notif_pipes[MAX_SESSION_COUNT];

char output[MAX_STRING_SIZE];

char buffer_server[MAX_SESSION_COUNT];

void close_clean_pipes(int position) {
  
  close(req_pipes[position]);
  close(res_pipes[position]);
  close(notif_pipes[position]);

  notif_pipes[position] = -1;
  res_pipes[position] = -1;
  req_pipes[position] = -1;

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


void sigusr1_handler(int sig) {
  (void)sig;

  printf("Signal handled\n");
  received_sigusr1 = 1;
  
}


int handle_client_subscription(char* buffer, int thread_id) {

    // Separar os campos do buffer
  
  char* token = strtok(buffer, "|");
  
  //write_str(STDOUT_FILENO, token);

  if (token == NULL) {
    fprintf(stderr, "Invalid message format (no OP_CODE).\n");
    return -1;
  }

  char* key = strtok(NULL, "|"); 
  //write_str(STDOUT_FILENO, key);
  
  if (key == NULL) {
        fprintf(stderr, "Invalid message format (missing KEY).\n");
        return -1;
  }

  int notif_id = notif_pipes[thread_id];

  //printf("NOTIF ID SUBSCRIBING: %d thread id: %d\n", notif_id, thread_id);
  
  return add_key_subscriber(key, notif_id);

}

int handle_client_unsubscription(char* buffer, int thread_id) {

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

  int notif_id = notif_pipes[thread_id];
  
  return remove_key_subscriber(key, notif_id);
}

int handle_client_unregister(int thread_id) {

 int notif_pipes_id = notif_pipes[thread_id];

 return disconnect_client(notif_pipes_id);
  
}

int handle_client_register(char* buffer, int thread_id) {
  
    // Separar os campos do buffer
    char* token = strtok(buffer, "|");
    if (token == NULL) {
        fprintf(stderr, "Invalid message format (no OP_CODE).\n");
        return -1;
    }

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
        close(responses_fd);
        return -1;
    }
    
    // Adds pipes to server
    req_pipes[thread_id] = requests_fd; 
    res_pipes[thread_id] = responses_fd;
    notif_pipes[thread_id] = notifications_fd;

    //printf("Requests pipe [%d]: %d\n", thread_id, req_pipes[thread_id]);
    //printf("Responses pipe [%d]: %d\n", thread_id, res_pipes[thread_id]);
    //printf("Notif pipe [%d]: %d\n", thread_id, notif_pipes[thread_id]);

    // FIFOs opened successfully
    return 0;
}

void client_session(int thread_id, char* buffer, ssize_t bytes_read) {



  int registration = 1;
  int result;

    while (server_on) {
      
      //printf("new iteration\n");
      //printf("Requests pipe [%d]: %d\n", thread_id, req_pipes[thread_id]);
      //printf("Responses pipe [%d]: %d\n", thread_id, res_pipes[thread_id]);
      //printf("Notif pipe [%d]: %d\n", thread_id, notif_pipes[thread_id]);
      if (registration) {       
        registration = 0;
      } else {
        //printf("Buffer_size = %ld\n", sizeof(buffer) - 1);
        bytes_read = read(req_pipes[thread_id], buffer, sizeof(buffer) - 1);
        //printf("Bytes_read: %ld e %s\n",bytes_read, buffer);
      }

      if (bytes_read - 1 < 0) {
        fprintf(stderr, "Invalid message size.\n");
        return;
      }
  
      buffer[bytes_read - 1] = '\0';

      // Process the request
      switch (buffer[0]) {

        case OP_CODE_CONNECT:
          //printf("CONNECT\n");
          //snprintf(output, MAX_WRITE_SIZE, "Pipe path: %s\n", buffer);
          //write_str(STDOUT_FILENO, output);

          if ((handle_client_register(buffer, thread_id)) == -1) {
            perror("Error handling client regist\n");
            write(res_pipes[thread_id] , "1|1", 4);
            continue;
          }

          write(res_pipes[thread_id] , "1|0", 4);

          break;
        
        case OP_CODE_DISCONNECT:
          //printf("DISCONNECT\n");
          result = handle_client_unregister(thread_id);
          
          if (result == -1) {
            perror("Error handling client disconnection\n");
            continue;
          }
          snprintf(output, sizeof(output), "2|%d", result);
          write(res_pipes[thread_id], output, 4);

          break;

        case OP_CODE_SUBSCRIBE:
          //printf("SUBSCRIBE\n");
          result = handle_client_subscription(buffer, thread_id);
          if (result == -1) {
            perror("Error handling client subscription\n");
            continue;
          }
          snprintf(output, sizeof(output), "3|%d", result);
          write(res_pipes[thread_id], output, 4);
          break;

        case OP_CODE_UNSUBSCRIBE:
          //printf("UNSUBSCRIBE\n");
          result = handle_client_unsubscription(buffer, thread_id);

          if (result == -1) {
            perror("Error handling client unsubscription\n");
            continue;
          }
          snprintf(output, sizeof(output), "4|%d", result);
          write(res_pipes[thread_id], output, 4);
          break;

        default:
          break;
      }
    }

    close_clean_pipes(thread_id);
}

void* client_thread(void* arg) {

  int thread_id = *((int *)arg);
  free(arg);

  while (1) {

    
      //printf("Thread %d: Waiting on semaphore\n", thread_id);
      sem_wait(&buffer_sem);
      active_sessions++;
      //printf("Thread %d: Got semaphore\n", thread_id);

      pthread_mutex_lock(&buffer_mutex);

      char buffer[MAX_WRITE_SIZE];
      strncpy(buffer, buffer_server, sizeof(buffer) - 1);

      //printf("\nRequest: %s on thread ID: %d\n", buffer_server, thread_id);

      pthread_mutex_unlock(&buffer_mutex);

      client_session(thread_id, buffer, sizeof(buffer) - 1);
      active_sessions--;

      sem_post(&sessions_sem);

      //printf("Client on thread %d Disconnected\n", thread_id);
    

  }

}

void reset_server() {

  print_subscriptions();
  printf("DETETOU ERRO SIGUSR1\n");
      // Limpar tudooooo
  received_sigusr1 = 0;
  server_on = 0;


  while (1) {
    if (active_sessions == 0) {
      printf("Sessões Terminadas!\n");
      remove_all_subscriptions();
      printf("Removeu Subscricoes\n");
      break;
    }
  }

  //print_subscriptions();

}

void clients_receiver(int reg_pipe_fd) {

  server_on = 1;

  while (1) {
    // print_subscriptions();
    if (received_sigusr1) {
      reset_server();
    }

    char local_buffer[MAX_PIPE_PATH_LENGTH*4];

    ssize_t reg_bytes_read = read(reg_pipe_fd, local_buffer, sizeof(local_buffer) - 1);

    if (reg_bytes_read > 1) {
      //write_str(STDOUT_FILENO, "A client wants to start a session\n");

      sem_wait(&sessions_sem);

      local_buffer[reg_bytes_read] = '\0';

      pthread_mutex_lock(&buffer_mutex);

      strcpy(buffer_server, local_buffer);

      sem_post(&buffer_sem);
      pthread_mutex_unlock(&buffer_mutex); 


    } else if (reg_bytes_read == 0) {
      sleep(5);
    } else {
        reset_server();
        continue;
    }
  }
  
}

static void dispatch_threads(DIR* dir, const char* register_pipe) {
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

  int reg_pipe_fd = open(register_pipe, O_RDONLY);
  
  if (reg_pipe_fd == -1) {
      perror("Failed to open FIFO for reading");
      closedir(dir);
      return;
  }

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &set, NULL);
  
  for (int i = 0; i < MAX_SESSION_COUNT; i++) {
    int* thread_id = malloc(sizeof(int));
    *thread_id = i;
    pthread_create(&client_threads[i], NULL, client_thread, thread_id);
  }

  printf("Já podes mandar o sinal!\n");

  pthread_sigmask(SIG_UNBLOCK, &set, NULL);

  clients_receiver(reg_pipe_fd);

  close(reg_pipe_fd);


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

int main(int argc, char** argv) {

  
  struct sigaction sa;
  sa.sa_handler = sigusr1_handler;
  sa.sa_flags = 0;
  sigemptyset(&sa.sa_mask);
  
  // Desbloquear SIGUSR1 apenas na tarefa anfitria

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

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  sem_init(&sessions_sem, 0, MAX_SESSION_COUNT);
  sem_init(&buffer_sem, 0, 0);

  dispatch_threads(dir, register_pipe);    
  
  pthread_mutex_destroy(&buffer_mutex);
  sem_destroy(&sessions_sem);
  sem_destroy(&buffer_sem);
  unlink(register_pipe); // Remover o FIFO

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
