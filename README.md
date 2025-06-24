## OC-P5

## Medical data migration project

**Table of contents**

- [Getting started](#getting-started)
	- [Building local image(optional)](#building-local-image)
	- [Installation with Docker/Docker Compose](#installation-with-docker-docker-compose)
- [Methodology](#methodology)
- [Authentification method](#authentification-method)
- [Custom Roles](#custom-roles)

## Getting started

### Building from local image (optional)
Needed only if you want to temper with src/config.py
```bash
    # clone git repository (make sure you're in the folder you want to put the aplication in)
    git clone https://github.com/SebM42/OC-P5
    # change to project root directory
    cd OC-P5
	# edit src/config.py if you want to
	# build image
	docker-compose build
```

### Installation with Docker/Docker Compose (docker engine required)

#### Set your MongoDB root password
Windows :
```cmd
    set MONGO_PASSWORD=your_password
```

MacOS/Linux :
```bash
    export MONGO_PASSWORD=your_password
```

#### Launch application
With migration :
```bash
    docker compose --profile migration up -d
```

Without migration (MongoDB only - in case you shut down your containers after the migration) : 
```bash
    docker compose up -d
```

## Methodology
Below is the procedural logic used by the system :
- containerize a persistent MongoDB database with authentification enabled
- creates a root user named 'admin' with password from the MONGO_PASSWORD environment variable
- containerize the migration application running on Python
- run src/main.py :
	- load data/healthcare_dataset.csv
	- standardize column names with only lower cases and _ instead of spaces
	- force data type of each column according to COLUMNS_DTYPE_TARGET in src/config.py
	- restructure the data to match the schema below :
	![banner](docs/images/schema.jpg)
		- by normalising the columns according to COLUMS_TO_NORMALISE in src/config.py
		- by serialising the columns according to COLUMNS_TO_SERIALISE in src/config.py
	- insert the restructured tables as collections in a new MongoDB database named after NEW_DB_NAME in src/config.py
	- create a view in MongoDB to get the flatten data (same structure as the data of origin)
	- create new roles in MongoDB according to ROLES in src/config.py
- run tests/integrity/*.py :
	- test integrity between origin data values and the new collections data values
	- test integrity between origin data types and the new collections data types

## Authentification method
The MongoDB database use a SCRAM-SHA-256 auth method :
SCRAM : Salted Challenge Response Authentication Mechanism
SHA-256 : Secure Hash Algorithm (256 refers to the hash length)

This method ensure a high level of security because :
- it never sends the real password on the network
- it uses a 256 Hash algorithm making it nearly impossible to crack as of today
- it protects against replay or brute force attacks

## Custom Roles
Along with the data migration, the script creates 3 custom new roles :
customRead : can only read the new database components (collections, documents, etc.)
customReadWrite : can only read or modify (insert/update) the new database components
customFull : can read, modify and delete the new database components

Each roles can only access the newly created database, they have no access to any other database or MongoDB system configuration