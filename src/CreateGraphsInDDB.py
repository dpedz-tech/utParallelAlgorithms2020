#!/usr/bin/env python
import os
import boto3
from sqlalchemy import create_engine
import pandas as pd
import json

# def downloadAndUnZipAction(year, quarter, airline, weight):
#
#
# class downloadAndUnZipDataFileThread(threading.Thread):
#     def __init__(self, year, quarter, airline, weight):
#         threading.Thread.__init__(self)
#         self.year = year
#         self.quarter = quarter
#         self.airline = airline
#         self.weight = weight
#
#     def run(self):
#         print("Starting " + str(self.year) + " " + str(self.quarter)+" " + str(self.airline)+ " " + str(self.weight))
#         downloadAndUnZipAction(self.currentYearPath, self.y, self.q)
#         print("Exiting " + str(self.year) + " " + str(self.quarter)+" " + str(self.airline)+ " " + str(self.weight))
#


def initializeDDB():
    print("Connecting to DB ")
    engine = create_engine(
        'postgresql+psycopg2://postgres:Rose1990!@aviationdbcluster.cluster-czl7eyzwjcyy.us-east-1.rds.amazonaws.com:5432/faa_database')
    client = boto3.client('s3')
    threads = []
    for y in range(2010, 2020):
        print(str(y))
        graph_miles = {}
        graph_fares = {}
        airline_df = pd.read_sql_query("select distinct \"Year\" as year, air.\"Code\"  as operating_airline_code " 
                                    "from market_data " 
                                    "join airlines air on market_data.\"OpCarrier\" = air.\"Code\" " 
                                    "where \"Year\" = "+str(y), con=engine)
        for index, row in airline_df.iterrows():
            print("Airline is "+str(row['operating_airline_code']))
            graph_miles['nodes']=[]
            graph_fares['nodes']=[]
            graph_miles['links']=[]
            graph_fares['links']=[]
            # Nodes
            airports_df = pd.read_sql_query("select distinct \"Year\",orgin.display_city_market_name_full as Airport, "
                                            "air.\"Code\" as Operating_Airline_Code "
                                            "from market_data "
                                            "JOIN airport_codes orgin on market_data.\"OriginAirportSeqID\" = orgin.airport_seq_id "
                                            "JOIN airlines air on market_data.\"OpCarrier\" = air.\"Code\" "
                                            "group by \"Year\", orgin.display_city_market_name_full, air.\"Code\" "
                                            "having \"Year\" = "+str(y)+ " and air.\"Code\" ='"+str(row['operating_airline_code'])+"' "
                                            "UNION DISTINCT "
                                            "select distinct \"Year\", dest.display_city_market_name_full  as Airport, "
                                            "air.\"Code\" as Operating_Airline_Code "
                                            "from market_data "
                                            "JOIN airport_codes dest on market_data.\"DestAirportSeqID\" = dest.airport_seq_id "
                                            "JOIN airlines air on market_data.\"OpCarrier\" = air.\"Code\" "
                                            "group by \"Year\", dest.display_city_market_name_full, air.\"Code\" "
                                            "having \"Year\" = "+str(y)+ " and air.\"Code\" ='"+str(row['operating_airline_code'])+"' ", con=engine)
            numberOfNodes =0
            for index, row in airports_df.iterrows():
                Node= {}
                Node["id"]=row['airport']
                Node["group"] =1
                graph_miles['nodes'].append(Node)
                graph_fares['nodes'].append(Node)
                print("Creating Node "+str(row['airport']))
                numberOfNodes+=1

            # Edges
            edges_df = pd.read_sql_query("select \"Year\", origin.display_city_market_name_full as Origin, "
                                    "dest.display_city_market_name_full as Destination, air.\"Code\" as Operating_Airline_Code, "
                                    "air.\"Description\" as Operating_Airline, "
                                    "avg(\"MktFare\") as MkFare, "
                                    "avg(\"MktMilesFlown\") as Miles "
                                    "from market_data "
                                    "JOIN airport_codes origin on market_data.\"OriginAirportSeqID\" = origin.airport_seq_id "
                                    "JOIN airport_codes dest on market_data.\"DestAirportSeqID\" = dest.airport_seq_id "
                                    "JOIN airlines air on market_data.\"OpCarrier\" = air.\"Code\" "
                                    "group by \"Year\", origin.display_city_market_name_full, "
                                    "dest.display_city_market_name_full, air.\"Description\",air.\"Code\" "
                                    "having \"Year\" = "+ str(y) +" and air.\"Code\" = '"+str(row['operating_airline_code'])+"'", con=engine)
            numberOfMileEdges = 0
            numberOfFaresEdges = 0
            for index, row in edges_df.iterrows():
                link_mile= {}
                link_mile["source"] = row['origin']
                link_mile["target"] = row['destination']
                link_mile["value"] = row['miles']
                link_fare= {}
                link_fare["source"] = row['origin']
                link_fare["target"] = row['destination']
                link_fare["value"] = row['mkfare']
                graph_miles['links'].append(link_mile)
                graph_fares['links'].append(link_fare)
                numberOfMileEdges+=1
                numberOfFaresEdges+=1
                print("Creating Edge between " + row['origin'] + " and " + row['destination'])
                graph_miles_filename = str(y)+"_"+str(row['operating_airline_code'])+"_"+str(numberOfNodes)+"_"+str(numberOfMileEdges)+"_miles.json"
                graph_fares_filename= str(y) + "_" + str(row['operating_airline_code']) + "_" + str(numberOfNodes) + "_" + str(numberOfFaresEdges) + "_fares.json"
            with open(graph_miles_filename, 'w' , encoding='utf-8') as outfile1:
                print("uploading miles json")
                # json.dump(graph_fares, outfile1, ensure_ascii=False, indent=4)
                # os.path.join(os.path.dirname(os.path.abspath(__file__)) + "/" + graph_fares_filename)
                response = client.put_object(
                    Body=json.dump(graph_fares),
                    Bucket='dale-aviation-graphs',
                    Key=graph_miles_filename,
                )
                print(response)

            with open(graph_fares_filename, 'w' , encoding='utf-8') as outfile2:
                print("uploading fares json")
                # json.dump(graph_fares, outfile2, ensure_ascii=False, indent=4)
                # os.path.join(os.path.dirname(os.path.abspath(__file__)) + "/" + graph_fares_filename)
                response = client.put_object(
                    Body=json.dump(graph_fares),
                    Bucket='dale-aviation-graphs',
                    Key=graph_fares_filename,
                )
                print(response)




            print("Done with Airline " + str(row['operating_airline_code']))
        print("Done with Year " + str(y))
    print("Script Done")


initializeDDB()