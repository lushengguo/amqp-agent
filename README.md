# amqp-agent
Receive data from another process and send data to rabbitmq  
Library guarenteened that all messages received will ultimatlly produce to rabbitmq (endless retry)  
Message will be save into memory and sqlite in any situation that didn't received confirmation    
Cached Message will be reproduced periodically  

You can configure the maximum memory usage and other log's config in config/deafault.toml (but no limit to disk, not impled yet)

I created this repo for one product of our company, it's didn't update frequently, but always has issue with producing message to rabbitmq, thought there are serveral AMQP library for cpp, but seldom of them are reliable and easy to use, AMQP-CPP needs you to impl network layer on your own on windows, hard coding that and parse amqp by hand (maintain it's state) is error prone.   
