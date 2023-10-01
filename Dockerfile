FROM python:3.8-slim

# Set working director and copy source code
WORKDIR /

COPY . /

# Install any needed packages specified in requirements.txt
RUN pip install confluent-kafka

# Run consumer.py when the container launches
CMD ["python", "consumer.py"]
