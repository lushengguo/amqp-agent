# amqp-agent
received data from another process and send data to rabbitmq  
auto reconnect to rabbitmq and storge data generated during connection broke into memory and sqlite  
you can configure the maximum memory usage of memory usage in config file

I created this repo for one product of our company, it's didn't update frequently, but always has issue with produce message to rabbitmq, thought there are serveral AMQP library for cpp, but seldom of them are reliable and easy to use, AMQP-CPP needs you to impl network layer on your own on windows, hard coding that and parse amqp by hand (maintain it's state) is error prone.   
