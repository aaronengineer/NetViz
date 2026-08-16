FROM python:3.12-slim
WORKDIR /app
COPY app.py sankey.html topology.demo.json netcommon.py inventory_store.py rest_api.py netprobe.py ./
EXPOSE 8766
CMD ["python3", "app.py"]
