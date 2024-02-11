#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <iostream>
#include <fstream>
using namespace std;
int countLine (char *filename) {
    std::ifstream in(filename, ios_base::in);
    if (!in.is_open()) return 0;

    int count = 0;
    std::string buffer;
    while (!in.eof()) {
        std::getline(in, buffer);
        count++;
    }

    return count;
}
int listDir (char *path) {
    DIR *pDir;
    struct dirent *ent;
    int i = 0;
    char childpath[512];
    int count = 0;

    pDir = opendir(path);
    memset(childpath, 0, sizeof(childpath));

    while ((ent = readdir(pDir)) != nullptr) {
        if (ent->d_type & DT_DIR) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0
                || strcmp(ent->d_name, "cmake-build-debug") == 0)
                continue;
            sprintf(childpath, "%s/%s", path, ent->d_name);
            printf("path:%s\n", childpath);

            count += listDir(childpath);
        } else {
            char filename[256];
            sprintf(filename, "%s/%s%c", path, ent->d_name, '\0');
            cout << ent->d_name << endl;
            count += countLine(filename);
        }
    }

    return count;
}

int main (int argc, char *argv[]) {
    std::cout << listDir("/root/share/kv-store/kv-store-server") << std::endl;
    return 0;
}