#include <boost/context/all.hpp>
#include <iostream>

int main() {
    int n=35;
    boost::context::execution_context<int> ctx1(
        [n](boost::context::execution_context<int> ctx2, int) mutable {
            int a=0;
            int b=1;
            while(n-- > 0){
                auto result=ctx2(a);
                ctx2=std::move(std::get<0>(result));

                auto next=a+b;
                a=b;
                b=next;
            }

            return ctx2;
    });

    for(int i=0;i<10;++i){
        auto result=ctx1(i);
        ctx1=std::move(std::get<0>(result));
        std::cout<<std::get<1>(result)<<" ";
    }
    
    return 0;
}