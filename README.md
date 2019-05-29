# WTS
The system will make 20% annual return

## 1. Roadmap

### 1.1. Daily Action Plan Meeting

Beijing time 22:00 on Tuesday, Wednesday, Thursday, Saturday and Sunday.

New York time 10:00 on Tuesday, Wednesday, Thursday, Saturday and Sunday.

### 1.2. Tasks and Schedule

## 2. Deployment Environment

- macOS

    > A setup.py is needed in the future.

    ```bash
    brew install python3
    pip3 install pycodestyle sqlalchemy tushare bs4 json
    ```

## 3. Code Covention

Follow PEP8 and enforce the code style checking by `pycodestyle`.

```bash
ln -s ../../pre-commit .git/hooks/pre-commit
```

## 4. System Design

- [Design Diagram](https://www.processon.com/view/link/5ca0e3d3e4b08743435e599c)

## 5. Database

#### PostgreSQL

##### 1. Install

> Do it only once.

```
brew install postgresql
pip3 install psycopg2

sudo install -o $USER -d /db_data
sudo install -o $USER -d /db_data/postgresql
pg_ctl init -D /db_data/postgresql
```

##### 2. Start PostgreSQL

> Do it when you could not connect to the database.

```
pg_ctl -D /db_data/postgresql start
```

##### 3. Stop PostgreSQL

```
pg_ctl -D /db_data/postgresql stop
```

