// derleme kodu : 
// gcc -o connectionpool main.c -I/usr/include/postgresql -L/usr/lib -lpq -g -pthread


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <libpq-fe.h>
#include <netinet/in.h>

#define MAX_CONNECTIONS 2
#define PORT 8080
#define BUFFER_SIZE 1024

typedef struct {
    PGconn *connections[MAX_CONNECTIONS];
    int count;
    pthread_mutex_t mutex;
} ConnectionPool;

typedef struct {
    int socket;
    ConnectionPool *pool;
} ConnectionArg;

int active_connections = 0;  // Aktif bağlantı sayısını tutan global değişken
pthread_mutex_t conn_count_mutex = PTHREAD_MUTEX_INITIALIZER;

ConnectionPool *create_pool(const char *conninfo) {
    ConnectionPool *pool = malloc(sizeof(ConnectionPool));

    if (!pool) {
        fprintf(stderr, "Bellek ayrımı başarısız\n");
        return NULL;
    }

    pool->count = 0;
    pthread_mutex_init(&pool->mutex, NULL);

    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        PGconn *conn = PQconnectdb(conninfo);
        if (PQstatus(conn) == CONNECTION_BAD) {
            fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(conn));
            PQfinish(conn);
            continue;
        }
        pool->connections[pool->count++] = conn;

        if (pool->count >= MAX_CONNECTIONS) {
            break;
        }
    }

    if (pool->count == 0) {
        pthread_mutex_destroy(&pool->mutex);
        free(pool);
        return NULL;
    }

    return pool;
}

PGconn *get_connection(ConnectionPool *pool) {
    pthread_mutex_lock(&pool->mutex);

    if (pool->count > 0) {
        PGconn *conn = pool->connections[--pool->count];
        pthread_mutex_unlock(&pool->mutex);
        return conn;
    } else {
        // Eğer bağlantı yoksa, null döndür
        printf("Bağlantı havuzunda yeterli bağlantı yok!\n");
        pthread_mutex_unlock(&pool->mutex);
        return NULL;
    }
}

void release_connection(ConnectionPool *pool, PGconn *conn) {
    pthread_mutex_lock(&pool->mutex);
    if (pool->count < MAX_CONNECTIONS) {
        pool->connections[pool->count++] = conn;
    } else {
        PQfinish(conn);
    }
    pthread_mutex_unlock(&pool->mutex);
}

void destroy_pool(ConnectionPool *pool) {
    if (pool) {
        for (int i = 0; i < pool->count; i++) {
            if (pool->connections[i]) {
                PQfinish(pool->connections[i]);
            }
        }
        pthread_mutex_destroy(&pool->mutex);
        free(pool);
    }
}

void *handle_connection(void *arg) {
    ConnectionArg *connection_arg = (ConnectionArg *)arg;
    int client_socket = connection_arg->socket;
    ConnectionPool *pool = connection_arg->pool;
    free(connection_arg);  // Arg'dan bellek serbest bırakılıyor

    // Eğer pool NULL ise hata mesajı yazdırın
    if (pool == NULL) {
        fprintf(stderr, "Hata: Bağlantı havuzu geçersiz!\n");
        close(client_socket);
        return NULL;
    }

    pthread_mutex_lock(&conn_count_mutex);
    active_connections++;  // Aktif bağlantı sayısını artır
    printf("Mevcut aktif bağlantılar: %d\n", active_connections);
    pthread_mutex_unlock(&conn_count_mutex);

    char buffer[BUFFER_SIZE] = {0};

    while (1) {
        memset(buffer, 0, BUFFER_SIZE);  // Tamponu temizle
        int valread = read(client_socket, buffer, BUFFER_SIZE - 1);  // Gelen veriyi oku

        if (valread <= 0) {
            printf("Bağlantı kapandı veya okuma hatası.\n");
            break;  // Bağlantı kapandıysa döngüden çık
        }

        buffer[valread] = '\0';  // Gelen veriyi sonlandır
        printf("Gelen sorgu: %s\n", buffer);

        // Veritabanı bağlantısı al
        PGconn *conn = get_connection(pool);
        if (conn == NULL) {
            const char *response = "Hata: Veritabanı bağlantısı alınamadı. Mevcut bağlantılar dolmuş olabilir.\n";
            send(client_socket, response, strlen(response), 0);
            continue;  // Bağlantı alımında hata varsa döngü devam etsin
        }

        // Sorguyu çalıştır
        PGresult *res = PQexec(conn, buffer);
        if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK) {
            const char *error_response = PQerrorMessage(conn);
            send(client_socket, error_response, strlen(error_response), 0);
        } else {
            int rows = PQntuples(res);
            int cols = PQnfields(res);
            char result[BUFFER_SIZE] = {0};  // Sonuçları saklayacak tampon

            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    strncat(result, PQgetvalue(res, i, j), BUFFER_SIZE - strlen(result) - 1);
                    strncat(result, "\t", BUFFER_SIZE - strlen(result) - 1);
                }
                strncat(result, "\n", BUFFER_SIZE - strlen(result) - 1);
            }
            send(client_socket, result, strlen(result), 0);
        }

        PQclear(res);  // Bellek temizliği
        release_connection(pool, conn);  // Bağlantıyı geri havuza bırak

        const char *prompt = "\nSorgu girin: ";
        send(client_socket, prompt, strlen(prompt), 0);
    }

    pthread_mutex_lock(&conn_count_mutex);
    active_connections--;  // Aktif bağlantı sayısını azalt
    printf("Bağlantı kapandı. Mevcut aktif bağlantılar: %d\n", active_connections);
    pthread_mutex_unlock(&conn_count_mutex);

    close(client_socket);  // Sonunda istemci bağlantısını kapat
    return NULL;
}

void start_server(ConnectionPool *pool) {
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    int max_connection_warning = 0;  // Uyarıyı kontrol etmek için değişken

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt error");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 3) < 0) {
        perror("listen failed");
        exit(EXIT_FAILURE);
    }

    printf("Sunucu başlatıldı. Port %d'yi dinliyor...\n", PORT);

    while (1) {
        pthread_mutex_lock(&conn_count_mutex);
        if (active_connections >= MAX_CONNECTIONS) {
            if (max_connection_warning == 0) {
                printf("Maksimum bağlantı sayısına ulaşıldı, yeni bağlantı kabul edilmiyor.\n");
                max_connection_warning = 1;  // Uyarıyı yazdırdık, değişkeni güncelle
            }
            pthread_mutex_unlock(&conn_count_mutex);
            continue;  // Eğer aktif bağlantılar maksimum sayıya ulaşmışsa, yeni bağlantıyı kabul etme
        }
        pthread_mutex_unlock(&conn_count_mutex);

        // Yeni bağlantı isteği bekleniyor
        if ((new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept failed");
            continue;
        }

        pthread_mutex_lock(&conn_count_mutex);
        if (active_connections >= MAX_CONNECTIONS) {
            close(new_socket);  // Bağlantıyı kapat
            pthread_mutex_unlock(&conn_count_mutex);
            continue;  // Yeniden kontrol et
        }
        pthread_mutex_unlock(&conn_count_mutex);

        ConnectionArg *connection_arg = malloc(sizeof(ConnectionArg));
        connection_arg->socket = new_socket;
        connection_arg->pool = pool;  // Connection pool'u geçiyoruz

        pthread_t thread_id;
        pthread_create(&thread_id, NULL, handle_connection, (void*)connection_arg);
        pthread_detach(thread_id);  // Bağlantıyı bağımsız olarak işleyin
    }
}

int main() {
    const char *conninfo = "user=postgres password=20022007 dbname=c_test_db sslmode=disable";
    ConnectionPool *pool = create_pool(conninfo);
    if (!pool) {
        fprintf(stderr, "Connection pool oluşturulamadı.\n");
        return EXIT_FAILURE;
    }

    start_server(pool);
    destroy_pool(pool);
    return EXIT_SUCCESS;
}