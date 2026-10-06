#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void *await_owner_exit(void *unused)
{
    char buffer[64];
    ssize_t bytes_read;

    do
    {
        bytes_read = read(STDIN_FILENO, buffer, sizeof(buffer));
    }
    while (bytes_read > 0 || (bytes_read < 0 && EINTR == errno));

    kill(getpid(), SIGTERM);
    return NULL;
}

__attribute__((constructor)) static void watch_owner(void)
{
    const char *flag = getenv("AERON_ELIXIR_OWNER_STDIN");

    if (NULL != flag && 0 == strcmp(flag, "1"))
    {
        pthread_t thread;

        if (0 == pthread_create(&thread, NULL, await_owner_exit, NULL))
        {
            pthread_detach(thread);
        }
    }
}
