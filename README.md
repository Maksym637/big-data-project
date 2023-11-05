# big-data-project
Big Data project to extract, transform and load IMDb dataset with PySpark
- - -
### Developed by

_Maksym637_, _augusto-alexus_, _RestingState_, _EyR1oN_, _PavloYend_, _m-gorg_
- - - 
### Dataset
Description: [IMDb Non-Commercial Dataset](https://developer.imdb.com/non-commercial-datasets/#titlebasicstsvgz)
- - -
### Set up project

#### Set up locally:
1. Install PySpark and run it with the command:
```shell
pyspark
```
2. Clone the provided repository
3. In the `utils` folder create the `constants.py` file with such content:
```python
"""Data input / output paths"""

INPUT_DATA_PATH = 'INPUT_DATA_PATH'
OUTPUT_DATA_PATH = 'OUTPUT_DATA_PATH'
```
4. Create `venv`, activate it, and install all dependencies:
```shell
pip install -r requirements.txt
```
5. Run project with the command:
- for _Linux_ users
```shell
make run-app-linux
```
- for _Windows_ users
```shell
make run-app-windows
```

#### Set up via Docker:
1. Install Docker
2. Pull docker image:
```shell
docker build -t my-spark-img .
```
3. Run image:
```shell
build -t my-spark-img .
```
- - -