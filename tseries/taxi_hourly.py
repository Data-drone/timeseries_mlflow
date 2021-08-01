from tseries.dataset import Dataset
from pyspark.sql import functions as F

class TaxiHourly(Dataset):

    """
    
    Builds out an hourly taxi dataset
    
    """

    def __init__(self, sparksession):

        """
        
        train_steps is for at least two weeks
        forecast_period is for 72 hours
        
        """

        self.sparksession = sparksession
        self.train_steps = 24*14
        self.forecast_period = 72
        
    def load_data(self):

        nyc_taxi_dataset = self.sparksession.sql("SELECT * FROM processed.nyc_taxi_dataset")
        self.dataset = (
            nyc_taxi_dataset
                .withColumn("pickup_date", F.date_trunc("pickup_datetime", "HOUR"))
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
