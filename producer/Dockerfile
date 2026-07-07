FROM python:3.11-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py simulate_devices.py ./

CMD ["python", "simulate_devices.py"]
