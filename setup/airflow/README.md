## 1. Create network

```shell
docker network create streaming-network --driver bridge
```

## 2. Run airflow

**Start ariflow**

Firstly, build a custom image using Dockerfile


```shell
docker build -t airflow-common .
```

```shell
docker compose up -d
```

**Đưa thư mục project vào trong AIRFLOW_PROJ_DIR ở .env**

