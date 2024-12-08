# healthcare-data-pipeline

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)

## Overview

This is your new Kedro project with Kedro-Viz and PySpark setup, which was generated using `kedro 0.19.10`.

Take a look at the [Kedro documentation](https://docs.kedro.org) to get started.

## Dataset Representation

![Dataset Relation](https://raw.githubusercontent.com/vamshigaddi/Data_Engineering_Using_Kedro/refs/heads/main/Dataset_Relation.png)

## Data Flow
![Data Flow](https://raw.githubusercontent.com/vamshigaddi/Data_Engineering_Using_Kedro/refs/heads/main/Diagram.png)
## Setup Instructions
Before installing the dependencies via requirements.txt, you need to set up the following environment:

### 1.Download Apache Spark:

- Download Apache Spark version 3.3.1 from the official Apache Spark downloads page(https://archive.apache.org/dist/spark/spark-3.3.1/). Choose the version spark-3.3.1-bin-hadoop2.7 (for Hadoop 2.x compatibility).
  
- Extract the downloaded file to a directory on your system (e.g., C:\spark on Windows or /opt/spark on Linux/Mac).
- Download WinUtils (for Windows users):
- click here to download (https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe)
- Download the winutils.exe binary for Hadoop 2.7 from the winutils repository.
- Place the winutils.exe file in the bin directory under your Spark installation (e.g., C:\hadoop\bin\winutils.exe on Windows).
- Download and Install JDK 19:
- click here and download(https://www.oracle.com/java/technologies/javase/jdk19-archive-downloads.html)
- Download the JDBC driver version 42.7.4 for connecting postgres database
- Link: (https://jdbc.postgresql.org/download/)

### Set Environment Variables:
- For setting environment variable I will strongly recommend to watch this tutorial
- link: [youtube](https://www.youtube.com/watch?v=OmcSTQVkrvo&t=694s)

### DataBase Connection
- For connecting to the database provide the credentials in credentials.yml
- create credentials.yml in the conf/base directory and place the credentials init.
#### Sparl.yml
- replace the these two files path with your system files path,just go and see the spark.yml you will get idea.
- Java_home path
- jdbc_driver path

## How to install dependencies
1.Clone the repository:
```bash
git clone https://github.com/vamshigaddi/Data_Engineering_Using_Kedro.git
cd Data_Engineering_Using_Kedro
```
2. create and activate virtual environment
``` bash
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate
``` 

To install them, run:

```
pip install -r requirements.txt
```

## How to run your Kedro pipeline

You can run your Kedro project with:

```
kedro run
```
### I have created two pipelines
- cleaning 
- combining
### For running Pipeline separately
``` bash
kedro run --pipeline cleaning
```
```bash
kedro run --pipeline combining
```
### Postgres_data
![Database](https://raw.githubusercontent.com/vamshigaddi/Data_Engineering_Using_Kedro/refs/heads/main/Database_scrnst.png)
### Jupyter
To use Jupyter notebooks in your Kedro project, you need to install Jupyter:

```
pip install jupyter
```

After installing Jupyter, you can start a local notebook server:

```
kedro jupyter notebook
```

### JupyterLab
To use JupyterLab, you need to install it:

```
pip install jupyterlab
```

You can also start JupyterLab:

```
kedro jupyter lab
```

### IPython
And if you want to run an IPython session:

```
kedro ipython
```

### How to ignore notebook output cells in `git`
To automatically strip out all output cell contents before committing to `git`, you can use tools like [`nbstripout`](https://github.com/kynan/nbstripout). For example, you can add a hook in `.git/config` with `nbstripout --install`. This will run `nbstripout` before anything is committed to `git`.

> *Note:* Your output cells will be retained locally.

## Package your Kedro project

[Further information about building project documentation and packaging your project](https://docs.kedro.org/en/stable/tutorial/package_a_project.html)
