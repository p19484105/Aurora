//
//  main.c
//  Aurora
//
//  Created by Paxton Rivera on 5/9/25.
//

#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>

int main(void) {
    // Source the .zshrc to set the environment variable
    FILE *fp = popen("source ~/.zshrc && /usr/bin/python3 /Users/paxtonrivera/Documents/Aurora/Aurora/async.py 2>&1", "r");
    if (!fp) return 1;

    char buffer[1024];
    while (fgets(buffer, sizeof(buffer), fp)) {
        fputs(buffer, stdout);
    }

    int status = pclose(fp);
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    return 1;
}
