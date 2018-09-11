#include <iostream>
#include <boost/asio/signal_set.hpp>
#include <boost/optional.hpp>

using namespace std;

int main() {
    cout << "creating service..." << endl;
    auto srv = std::make_shared<boost::asio::io_service>();

    boost::optional

    std::shared_ptr<boost::asio::signal_set> sigint_set(
        new boost::asio::signal_set(*srv, SIGINT));
    sigint_set->async_wait([sigint_set, srv](const boost::system::error_code& err, int num) {
        cout << "stopping\n";
        srv->stop();
        sigint_set->cancel();
    });

    srv->run();
    cout << "exited\n";
    return 0;
}