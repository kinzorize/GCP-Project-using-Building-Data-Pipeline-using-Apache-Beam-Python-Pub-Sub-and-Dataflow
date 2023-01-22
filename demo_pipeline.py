
import apache_beam as beam
# Basically i am reading the csv file and doing a count in this demo code
pipeline1 = beam.Pipeline()


airline_count = (
    pipeline1
    | beam.io.ReadFromText('/Users/ghost/Downloads/Data_2/flights_data/flights.csv')
    | beam.Map(lambda line: line.split(','))
    | beam.Filter(lambda line: line[0] == '2015')
    | beam.Map(lambda line: (line[4], 1))
    | beam.CombinePerKey(sum)
    | beam.io.WriteToText('/Users/ghost/Documents/gcp_project/GCP-Project-using-Building-Data-Pipeline-using-Apache-Beam-Python-Pub-Sub-and-Dataflow/output')
)

pipeline1.run()
