Homework assignment from CS 5260 using AWS

Run program as follows

python ./consumer.py <request-bucket> <bucket/db> <destination name> <sqs/bucket>
  
for bucket destination using sqs

python ./consumer.py <usu-cs5260-big-requests bucket usu-cs5260-big-web sqs

for bucket destination without sqs

python ./consumer.py <usu-cs5260-big-requests bucket usu-cs5260-big-web bucket
                                              
for database destination using sqs

python ./consumer.py usu-cs5260-big-requests db widgets sqs

for database destination without sqs

python ./consumer.py usu-cs5260-big-requests db widgets bucket
