from jsonmanipulations.jsontagvariables import JsonTagVariables as JsonTagVar
from jsonmanipulations.configparametervalue import ConfigurationParametersValue as CPV
from pyspark_components.spark_utilities import spark_utilities
from root.commonvariables import CommonVariables
from utility.utils import  Common_Utils_Operations as CUO 
from utility.pandas_utilities import Pandas_Utilities as PU 


class trigger_pipeline:
    def __init__(self, pipeline_name, pipeline_parameters):
        self.pipeline_name = pipeline_name
        self.pipeline_parameters = pipeline_parameters

    def run(self):
        # Trigger the pipeline
        print(f"Triggering the pipeline {self.pipeline_name} with parameters {self.pipeline_parameters}")
        # Code to trigger the