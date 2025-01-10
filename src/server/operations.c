#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "constants.h"
#include "io.h"
#include "kvs.h"
#include "operations.h"

static struct HashTable *kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

SubscriptionEntry* head = NULL;

SubscriptionEntry* find_entry(const char* key) {
  
  SubscriptionEntry* current = head;
  while (current != NULL) {
    if (strcmp(current->key, key) == 0) {
      return current;
    }
    current = current -> next;
  }
  return NULL;
}

SubscriptionEntry* add_entry(const char* key) {
  
  SubscriptionEntry* new_entry = (SubscriptionEntry*)malloc(sizeof(SubscriptionEntry));

  if (!new_entry) {
    perror("Failed to allocate memory for SubscriptionEntry");
    return head; // Retorna a lista original
  }

  // Inicializar os campos do nó
  strncpy(new_entry->key, key, MAX_STRING_SIZE - 1);
  new_entry->key[MAX_STRING_SIZE - 1] = '\0'; // Garantir terminação
  new_entry->head_client = NULL;
  new_entry->next = head; // Aponta para o antigo cabeçalho

  return new_entry; // Retorna o novo cabeçalho
}

SubscriptionEntry* remove_entry(const char* key) {

  if (head == NULL) {
    return NULL;
  }

  SubscriptionEntry* curr = head;
  SubscriptionEntry* prev = NULL;

  while (curr != NULL) {
      if (strcmp(curr->key, key) == 0) {
        if (prev == NULL) {
          
          head = curr->next;
        } else {
          prev->next = curr->next;
        }
      }
  }

  ClientNode* client = curr->head_client;
  
  while (client != NULL) {
    ClientNode* next_client = client->next;
    free(client);
    client = next_client;

  }
  free(curr);

  return head;

}

int add_key_subscriber(const char* key, int notif_id) {

  SubscriptionEntry* entry = find_entry(key);

  if (entry == NULL) {
    return 1;
  }
  // Criar um novo cliente
  ClientNode* new_client = (ClientNode*)malloc(sizeof(ClientNode));
  
  if (!new_client) {
      perror("Failed to allocate memory for ClientNode");
      return 1; // Retorna erro se a alocação falhar
  }

  new_client->notif_fd = notif_id; // Define o ID de notificação
  new_client->next = entry->head_client; // Insere no início da lista de clientes
  entry->head_client = new_client; // Atualiza o ponteiro para a lista de clientes

  return 0; // Sucesso
}

int remove_key_subscriber(const char* key, int notif_id) {
  
  if (head == NULL || key == NULL) {
      return -1;
  }

  SubscriptionEntry* entry = find_entry(key);
  if (entry == NULL) {
      return -1;
  }

  ClientNode* current = entry->head_client;
  ClientNode* prev = NULL;

  while (current != NULL) {
      if (current->notif_fd == notif_id) {
          if (prev == NULL) {
              entry->head_client = current->next;
          } else {
              
              prev->next = current->next;
          }

          free(current);
          return 0; 
      }

      prev = current;
      current = current->next;
  }

  return -1;
}

int disconnect_client(int notif_id) {

  SubscriptionEntry* entry = head;

    while (entry != NULL) {
        
        ClientNode* prev_client = NULL;
        ClientNode* client = entry->head_client;
        
        while (client != NULL) {

            if (client->notif_fd == notif_id) {
                
                if (prev_client == NULL) {
                    
                    entry->head_client = client->next;
                } else {
                    
                    prev_client->next = client->next;
                }

                free(client);
                break; 
            }

            prev_client = client;
            client = client->next;
        }
        
        entry = entry->next;
    }

    return 0;

}

void notify_clients(SubscriptionEntry* entry, const char* new_value) {

  ClientNode* curr_client = entry->head_client;
  
    while (curr_client != NULL) {

        // Enviar a mensagem de notificação
        char notification[MAX_STRING_SIZE*2 + 4]; // Espaço suficiente para chave, valor e parênteses
        snprintf(notification, sizeof(notification), "(%s,%s)", entry->key, new_value);

        ssize_t bytes_written = write(curr_client->notif_fd, notification, strlen(notification));
        if (bytes_written < 0) {
            perror("Failed to write to notification pipe\n");
        } else {
            printf("Notification sent to client pipe %d\n", curr_client->notif_fd);
        }

        // Passar para o próximo cliente
        curr_client = curr_client->next;
    }

}


int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  kvs_table = NULL;
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_wrlock(&kvs_table->tablelock);

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write key pair (%s,%s)\n", keys[i], values[i]);
    }
    
    SubscriptionEntry* entry = find_entry(keys[i]);

    if (entry == NULL) {
      head = add_entry(keys[i]);
    } else {
      notify_clients(entry, values[i]);
    }
  }


  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  
  pthread_rwlock_rdlock(&kvs_table->tablelock);

  write_str(fd, "[");
  for (size_t i = 0; i < num_pairs; i++) {
    char *result = read_pair(kvs_table, keys[i]);
    char aux[MAX_STRING_SIZE];
    if (result == NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s,KVSERROR)", keys[i]);
    } else {
      snprintf(aux, MAX_STRING_SIZE, "(%s,%s)", keys[i], result);
    }
    write_str(fd, aux);
    free(result);
  }
  write_str(fd, "]\n");
  
  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  
  pthread_rwlock_wrlock(&kvs_table->tablelock);

  int aux = 0;
  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i])) {
      if (!aux) {
        write_str(fd, "[");
        aux = 1;
      }
      char str[MAX_STRING_SIZE];
      snprintf(str, MAX_STRING_SIZE, "(%s,KVSMISSING)", keys[i]);
      write_str(fd, str);
    } else {
      head = remove_entry(keys[i]);
      SubscriptionEntry* entry = find_entry(keys[i]);
      notify_clients(entry, "DELETED");
    }
    
  }
  if (aux) {
    write_str(fd, "]\n");
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

void kvs_show(int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return;
  }
  
  pthread_rwlock_rdlock(&kvs_table->tablelock);
  char aux[MAX_STRING_SIZE];
  
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
    while (keyNode != NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s, %s)\n", keyNode->key, keyNode->value);
      write_str(fd, aux);
      keyNode = keyNode->next; // Move to the next node of the list
    }
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
}

int kvs_backup(size_t num_backup,char* job_filename , char* directory) {
  pid_t pid;
  char bck_name[50];
  snprintf(bck_name, sizeof(bck_name), "%s/%s-%ld.bck", directory, strtok(job_filename, "."),
           num_backup);

  pthread_rwlock_rdlock(&kvs_table->tablelock);
  pid = fork();
  pthread_rwlock_unlock(&kvs_table->tablelock);
  if (pid == 0) {
    // functions used here have to be async signal safe, since this
    // fork happens in a multi thread context (see man fork)
    int fd = open(bck_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    for (int i = 0; i < TABLE_SIZE; i++) {
      KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
      while (keyNode != NULL) {
        char aux[MAX_STRING_SIZE];
        aux[0] = '(';
        size_t num_bytes_copied = 1; // the "("
        // the - 1 are all to leave space for the '/0'
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        keyNode->key, MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        ", ", MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        keyNode->value, MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        ")\n", MAX_STRING_SIZE - num_bytes_copied - 1);
        aux[num_bytes_copied] = '\0';
        write_str(fd, aux);
        keyNode = keyNode->next; // Move to the next node of the list
      }
    }
    exit(1);
  } else if (pid < 0) {
    return -1;
  }
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}
