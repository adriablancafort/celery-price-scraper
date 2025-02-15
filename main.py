from celery import Celery
from dotenv import load_dotenv
import os


load_dotenv()

rabbit_ip = os.getenv("RABBITMQ_IP")
rabbit_port = os.getenv("RABBITMQ_PORT")
rabbit_user = os.getenv("RABBITMQ_USER")
rabbit_password = os.getenv("RABBITMQ_PASSWORD")

broker_url = f"amqp://{rabbit_user}:{rabbit_password}@{rabbit_ip}:{rabbit_port}//"

app = Celery("tasks", broker=broker_url)