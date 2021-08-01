from tseries.dataset import Dataset
from pyspark.sql import functions as F

class TaxiDaily(Dataset):
    """
    
    Loads the NYC Taxi dataset 

    TODO - add a self object for time column and column we forecast on

    """

    def __init__(self, sparksession):

        """
        train steps is for at least six months
        forecast_period is for 14 days

        """

        self.sparksession = sparksession
        self.train_steps = 6*30
        self.forecast_period = 14

    def load_data(self):

        nyc_taxi_dataset = self.sparksession.sql("SELECT * FROM processed.nyc_taxi_dataset")
        self.dataset = (
            nyc_taxi_dataset
                .withColumn("pickup_date", F.to_date("pickup_datetime"))
                .select("pickup_date", "total_amount")
                .groupBy("pickup_date")
                .agg(
                    F.count("total_amount").alias("total_rides"),
                    F.sum("total_amount").alias("total_takings")
                )
                .sort("pickup_date")
        )

        self.time_column = 'pickup_date'
        self.columns_to_predict = ['total_rides', 'total_takings']

        #self.train_data = (
        #    self.dataset
        #        .select(
        #            F.col("pickup_date").alias("ds"),
        #            F.col("total_rides").alias("y")
        #            )
        #        .filter("pickup_date < '2014-07-01'"))

        #self.test_data = (
        #    self.dataset
        #        .select(
        #            F.col("pickup_date").alias("ds"),
        #            F.col("total_rides").alias("y")
        #            )
        #        .filter("pickup_date >= '2014-07-01'"))