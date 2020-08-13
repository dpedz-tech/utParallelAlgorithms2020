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

# 'UA','WN','AA','DL','AS',
def initializeDDB():
    print("Connecting to DB ")
    engine = create_engine(
        'postgresql+psycopg2://postgres:PASSWORD@aviationdbcluster.cluster-czl7eyzwjcyy.us-east-1.rds.amazonaws.com:5432/faa_database')
    client = boto3.client('s3')
    threads = []
    for y in range(2019, 2020):
        print(str(y))
        graph_miles = {}
        graph_fares = {}
        node_dict ={}
        airline_df = pd.read_sql_query("select distinct \"Year\" as year, air.\"Code\"  as operating_airline_code "
                                       "from market_data "
                                       "join airlines air on market_data.\"OpCarrier\" = air.\"Code\" "
                                       "where \"Year\" = " + str(y) +
                                       " and air.\"Code\" in ( 'UA','WN','AA','DL','AS') ", con=engine)
        for airline_index, airline_row in airline_df.iterrows():
            print("Airline is " + str(airline_row['operating_airline_code']))
            graph_miles['nodes'] = []
            graph_fares['nodes'] = []
            graph_miles['links'] = []
            graph_fares['links'] = []
            # Nodes
            airports_df = pd.read_sql_query(
                # "select distinct \"Year\",CONCAT(orgin.display_airport_name,'-',orgin.display_city_market_name_full) as Airport, "
                "select distinct \"Year\",orgin.airport as Airport "
                # "orgin.airport_id as Airport_Code "
                "from market_data "
                "JOIN airport_codes orgin on market_data.\"OriginAirportID\" = orgin.airport_id "
                "JOIN airlines air on market_data.\"OpCarrier\" = air.\"Code\" "
                "group by \"Year\",orgin.airport, air.\"Code\" "
                "having \"Year\" = " + str(y) + " and air.\"Code\" ='" + str(airline_row['operating_airline_code']) + "' "
                                                                                                              "UNION DISTINCT "
                                                                                                              "select distinct \"Year\",dest.airport as Airport "
                                                                                                              # "dest.airport_id as Airport_Code "
                                                                                                              "from market_data "
                                                                                                              "JOIN airport_codes dest on market_data.\"DestAirportID\" = dest.airport_id "
                                                                                                              "JOIN airlines air on market_data.\"OpCarrier\" = air.\"Code\" "
                                                                                                              "group by \"Year\", dest.airport, air.\"Code\" "
                                                                                                              "having \"Year\" = " + str(y) + " and air.\"Code\" ='" + str(airline_row['operating_airline_code']) + "' ", con=engine)
            numberOfNodes = 0
            for airport_index, airport_row in airports_df.iterrows():
                Node = {}
                Node["name"] = airport_row['airport']
                # Node["node_id"] = airport_row['airport_code']
                node_dict[airport_row['airport']] = numberOfNodes
                Node["index"] = numberOfNodes
                Node["group"] = 1
                graph_miles['nodes'].append(Node)
                graph_fares['nodes'].append(Node)
                print("Creating Node " + str(airport_row['airport']))
                numberOfNodes += 1
            print("Done with Nodes")
            # Edges
            edges_df = pd.read_sql_query("select \"Year\", "
                                         # "origin.display_city_market_name_full as Origin_City," 
                                         "origin.airport as Origin_Airport,"
                                         # "origin.display_airport_name as Origin_Airport, "
                                         # "origin.airport_id as Origin_Airport_Code, "
                                         # "dest.display_city_market_name_full as Destination_City, "
                                         "dest.airport as Destination_Airport, "
                                         # "dest.display_airport_name as Destination_Airport, "
                                         # "dest.airport_id as Destination_Airport_Code, "
                                         # "air.\"Code\" as Operating_Airline_Code, "
                                         "air.\"Description\" as Operating_Airline, "
                                         # "avg(\"MktFare\") as MkFare, "
                                         "avg(\"MktMilesFlown\") as Miles "
                                         "from market_data "
                                         "JOIN airport_codes origin on market_data.\"OriginAirportID\" = origin.airport_id "
                                         "JOIN airport_codes dest on market_data.\"DestAirportID\" = dest.airport_id "
                                         "JOIN airlines air on market_data.\"OpCarrier\" = air.\"Code\" "
                                         "group by \"Year\", origin.airport, air.\"Code\", "
                                         "dest.airport, air.\"Description\" "
                                         "having \"Year\" = " + str(y) + " and air.\"Code\" = '" + str(airline_row['operating_airline_code']) + "'", con=engine)
            numberOfMileEdges = 0
            numberOfFaresEdges = 0
            for market_index, market_row in edges_df.iterrows():
                link_mile = {}
                link_mile["source"] = market_row['origin_airport']
                link_mile["target"] = market_row['destination_airport']
                link_mile["value"] = market_row['miles']
                # link_mile["source_airport_name"] = market_row['origin_airport']
                # link_mile["source_airport_city"] = market_row['origin_city']
                # link_mile["target_airport_name"] = market_row['destination_airport']
                # link_mile["target_airport_city"] = market_row['destination_city']


                # link_fare = {}
                # link_fare["source"] = market_row['origin_airport']
                # link_fare["target"] = market_row['destination_airport']
                # link_fare["value"] = market_row['mkfare']
                # link_fare["source_airport_name"] = market_row['origin_airport']
                # link_fare["source_airport_city"] = market_row['origin_city']
                # link_fare["target_airport_name"] = market_row['destination_airport']
                # link_fare["target_airport_city"] = market_row['destination_city']

                isDuplicate = False

                for edge in graph_miles["links"]:
                    if edge["source"] == market_row['destination_airport'] and edge["target"] == market_row['origin_airport']:
                        isDuplicate = True
                        break

                if not isDuplicate:
                    graph_miles['links'].append(link_mile)
                    # graph_fares['links'].append(link_fare)

                graph_miles["airline"] = market_row['operating_airline']
                # graph_fares['airline'] = market_row['operating_airline']
                numberOfMileEdges += 1
                numberOfFaresEdges += 1
                print("Creating Edge between " + str(market_row['origin_airport']) + " and " + str(market_row['destination_airport']))
                # graph_miles_filename = str(y) + "_" + str(market_row['operating_airline']) + "_" + str(
                #     numberOfNodes) + "_" + str(numberOfMileEdges) + "_miles.json"
                # graph_fares_filename = str(y) + "_" + str(market_row['operating_airline']) + "_" + str(
                #     numberOfNodes) + "_" + str(numberOfFaresEdges) + "_fares.json"
            # with open(graph_miles_filename, 'w', encoding='utf-8') as outfile1:
            #     print("uploading miles json")
            #     # json.dump(graph_fares, outfile1, ensure_ascii=False, indent=4)
            #     # os.path.join(os.path.dirname(os.path.abspath(__file__)) + "/" + graph_fares_filename)
            #     response = client.put_object(
            #         Body=json.dumps(graph_miles),
            #         Bucket='dale-aviation-graphs',
            #         Key=graph_miles_filename,
            #     )
            #     print(response)

            graph_miles_filename = str(y) + "_" + str(market_row['operating_airline']) + "_" + str(numberOfNodes) + "_" + str(numberOfMileEdges) + "_miles.text"
            with open(graph_miles_filename, 'w', encoding='utf-8') as outfile2:
                print("uploading miles text")
                # json.dump(graph_fares, outfile1, ensure_ascii=False, indent=4)
                # os.path.join(os.path.dirname(os.path.abspath(__file__)) + "/" + graph_fares_filename)
                # bodyOfFile = "Nodes\n"
                bodyOfFile = "\n"
                # for node in graph_miles["nodes"]:
                    # bodyOfFile += str(node["index"]) + "|" + node["name"] + "\n"
                # bodyOfFile += "Edges\n"
                for edge in graph_miles["links"]:
                    bodyOfFile += "edges.push_back(edge{"+str(node_dict[edge["source"]])+","+str(node_dict[edge["target"]])+","+str(int(edge["value"]))+"});\n"
                        # str(node_dict[edge["source"]]) + "|" + str(node_dict[edge["target"]]) + "|" + str(edge["value"]) + "|" + edge["source"] + "|" + edge["target"] + "\n"
                # bodyOfFile += "Operator\n"
                # bodyOfFile += graph_miles["airline"] + "\n"
                response = client.put_object(
                    Body=bodyOfFile,
                    Bucket='dale-aviation-graphs',
                    Key=graph_miles_filename,
                )
                print(response)

            # with open(graph_fares_filename, 'w', encoding='utf-8') as outfile3:
            #     print("uploading fares json")
            #     # json.dump(graph_fares, outfile2, ensure_ascii=False, indent=4)
            #     # os.path.join(os.path.dirname(os.path.abspath(__file__)) + "/" + graph_fares_filename)
            #     response = client.put_object(
            #         Body=json.dumps(graph_fares),
            #         Bucket='dale-aviation-graphs',
            #         Key=graph_fares_filename,
            #     )
            #     print(response)
            #
            # graph_fares_filename = str(y) + "_" + str(market_row['operating_airline_code']) + "_" + str(
            #     numberOfNodes) + "_" + str(numberOfFaresEdges) + "_fares.csv"
            # with open(graph_fares_filename, 'w', encoding='utf-8') as outfile4:
            #     print("uploading fares csv")
            #     # json.dump(graph_fares, outfile1, ensure_ascii=False, indent=4)
            #     # os.path.join(os.path.dirname(os.path.abspath(__file__)) + "/" + graph_fares_filename)
            #     bodyOfFile = "Nodes\n"
            #     for node in graph_fares["nodes"]:
            #         bodyOfFile += str(node["index"]) + "|" + node["name"] + "|" + str(node["node_id"]) + "\n"
            #     bodyOfFile += "Edges\n"
            #     for edge in graph_fares["links"]:
            #         bodyOfFile += str(node_dict[edge["source"]]) + "|" + str(node_dict[edge["target"]]) + "|" + str(edge["value"]) + "|" + edge["source_airport_name"] + "|" + edge["source_airport_city"] + "|" + edge["target_airport_name"] + "|" + edge["target_airport_city"] + "\n"
            #     bodyOfFile += "Operator\n"
            #     bodyOfFile += graph_fares["airline"] + "\n"
            #     response = client.put_object(
            #         Body=bodyOfFile,
            #         Bucket='dale-aviation-graphs',
            #         Key=graph_fares_filename,
            #     )
            #     print(response)

            print("Done with Airline " + str(market_row['operating_airline']))
        print("Done with Year " + str(y))
    print("Script Done")


initializeDDB()
