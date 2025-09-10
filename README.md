# SustainableCompetition
## Project Setup

1. Clone the repository:
   ```
   git clone sustainablecompetition_url
   cd sustainablecompetition
   ```
2. Run the setup script to configure Git filters and restore the database:
   ```
   ./setup.sh
   ```
3. The database will be automatically restored from the dump.

## random remarks

you can fetch the path of the database with :

````
db_path = importlib.resources.files("sustainablecompetition.data").joinpath("sustainablecompetition.db")
```