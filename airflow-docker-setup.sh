#! /usr/bin/bash
 
CONTAINER_ID=$(docker ps | grep worker | cut -c1-10)

if [ "$1" == "id" ]
then
	echo $CONTAINER_ID
	exit
fi

if [ -z "$CONTAINER_ID" ]
    then
	echo "No worker running. Run docker compose up in current directory first."
	exit
fi

docker exec -it -u 0 $CONTAINER_ID bash -c "curl \"https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip\" -o \"awscliv2.zip\"; sudo apt update ; sudo apt install unzip ; sudo unzip \"awscliv2.zip\" ; ./aws/install"


AWS_CREDENTIALS="credentials"

if [ -e "$AWS_CREDENTIALS" ] && [ -f "$AWS_CREDENTIALS" ]
then 
	docker cp $AWS_CREDENTIALS $CONTAINER_ID:/opt/airflow
	docker exec -it -u 0 $CONTAINER_ID bash -c "mkdir /home/airflow/.aws ; mv /opt/airflow/$AWS_CREDENTIALS /home/airflow/.aws"
else 
	echo "No credentials provided"
fi





