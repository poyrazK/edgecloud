#include <stdio.h>
int main() {
    struct sockaddr_in addr;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    bind(fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(fd, 128);
    int client = accept(fd, NULL, NULL);
    struct hostent *h = gethostbyname("api.example.com");
    (void)h;
    return 0;
}
