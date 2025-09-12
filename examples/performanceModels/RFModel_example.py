from sustainablecompetition.performancemodels.randomforestmodel import RandomForestPerformanceModel
import polars as pl
from sustainablecompetition.dataadaptors.csv_dataadaptor import CsvDataAdaptor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

data = CsvDataAdaptor(pl.DataFrame())
data.get_performances("environments.csv", "instances.csv", "solvers.csv", "performances.csv")
# Drop hash columns and status, keep only features and perf
X = data.df.drop(["env_hash", "inst_hash", "solver_hash", "status", "perf"])
# for this test, also drop non numerical data TODO handle them better
print(X.columns)
X = X.drop(["kernel_version", "activated_modules", "cpu_brand", "cpu_model", "solver_name"])
y_perfs = data.df["perf"].to_numpy()
y_status = data.df["status"].to_numpy()
# Initialize
rf = RandomForestPerformanceModel()
X_train, X_test, y_train, y_test = train_test_split(X, y_perfs, test_size=0.2, random_state=42)
rf.train(X_train, y_train)
y_pred = rf.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
print(f"MSE predict performance: {mse}")