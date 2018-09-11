#include <iostream>
#include <tuple>
#include <string>

using namespace std;

int main() {
    using t = tuple<int, string>;

    t t1(1, "hello world");

    auto e1(get<0>(t1));
    auto e2(get<1>(t1));

    // int i = 1;
    // auto e(get<i>(t1));

    cout << e1 << endl;
    cout << e2 << endl;

    auto t20 = 2;
    auto t21 = "tuple";
    auto t2 = tie(t20, t21);
    cout << get<1>(t2) << endl;

    string s3;
    std::tie(std::ignore, s3) = std::make_pair(3, "3");
    cout << s3 << endl;

    return 0;
}