# ETL com Airflow

Projeto de ETL utilizando **Apache Airflow** e **PostgreSQL**, seguindo uma arquitetura de medalhão (**Bronze → Silver → Gold**) e utilizando a modelagem **Star Schema**.

## Pré-requisitos

- Docker
- Docker Compose
- Git

---

## Como rodar o projeto

### Clonar o repositório
```bash
git clone <https://github.com/pedrolucasgp/etl-airflow>
cd etl-airflow
```

### Criar e ativar o ambiente virtual
```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### Instalar bibliotecas requiridas
```bash
pip install -r requirements.txt
```

### Subir os containers
```bash
docker compose up -d
```

A primeira inicialização pode demorar alguns minutos.

---

## Acessos

- **Airflow UI**: http://localhost:8080  
  Usuário: `airflow`  
  Senha: `airflow`

- **PostgreSQL**:
  - Host: `localhost`
  - Porta: `5432`
  - Banco: `airflow`
  - Usuário: `airflow`
  - Senha: `airflow`

---

## Executando as DAGs

1. Acesse o Airflow UI
2. Ative a DAG desejada (`bronze_dag`, `silver_dag`, `gold_dag`)
3. Execute manualmente ou aguarde o agendamento

---

## Tecnologias

- Apache Airflow
- PostgreSQL
- Docker / Docker Compose
- Python
