#include <iostream>
#include <vector>

using std::cout;
using std::endl;
using std::vector;

template<typename Plugin>
int find_plugin() {
    return sizeof(Plugin);
}

void init_impl(vector<int> v) {
    for (auto i : v) {
        cout << i;
    }
}

template<typename... Plugins>
void init() {
    init_impl({find_plugin<Plugins>()...});
}

int main() {
    init<char, short, int, long>();
    cout << endl;
    return 0;
}