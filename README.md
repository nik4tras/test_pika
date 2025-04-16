# Rabbitmq Pika to Oracle with Docker

## Preparation

### Docker

see [official documentation](https://docs.docker.com/engine/install/ubuntu/) and [post install documentation](https://docs.docker.com/engine/install/linux-postinstall/).

```bash
# uninstall all conflicting packages
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do sudo apt-get remove $pkg; done

# setting up the apt repository

# Add Docker's official GPG key:
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

# install software
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# post install

sudo groupadd docker
sudo usermod -aG docker $USER

# logout and log back in to activate group

# activate docker with systemd
sudo systemctl enable docker.service
sudo systemctl enable containerd.service
```

Setup custom docker network bridge

```bash
docker network create --driver bridge pika_test
```

### Python

Install Python packages

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv python3-dev python3-setuptools python3-wheel python-is-python3
```

Create venv and install python pip packages

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## OracleDB

1. Start OracleDB container: `./start_oracle.sh`
2. Connect to the Database as `system` using Dbeaver, VS Code or something else
    - Username: `system`
    - Password: `p123`
    - Host: `localhost`
    - Port: `1521`
    - Service Name: `FREEPDB1`
3. Create a new user and grant privileges by running [00_setup.sql](./oradb/00_setup.sql)
4. Connect as `test` and run [01_table_selects.sql](./oradb/01_table_selects.sql) to create the table and have some SQL stmts to test

## Container

### Building

For the producer, just run the script `./build_producer.sh`.

RabbitMQ and Oracle are used from the official Docker images.

## Running

1. Start Database: `./start_oracledb.sh`
2. Start RabbitMQ: `./start_rabbitmq.sh`
3. Start the producer: `./start_producer.sh`

To stop, just write

- for Oracle `docker stop oradb`
- for RabbitMQ `docker stop rabbitmq`

The producer stops when he has send all messages.
