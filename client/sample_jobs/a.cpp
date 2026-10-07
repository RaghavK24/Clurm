#include <iostream>
#include <unistd.h>  

using namespace std;

int main() {
    for (int i = 1; i <= 5; i++) {
        cout << i << endl;
        sleep(2); 
    }
    return 0;
}