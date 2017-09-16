using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Neo4jClient;
using System.Data;
using Newtonsoft.Json;
using System.IO;
using System.Text.RegularExpressions;
using CsvHelper;
namespace Neo4JTest
{
    class Program
    {
        public static DBInterface dbHandler = new DBInterface(".", "Freebase");
        static GraphClient neo4jClient;

            int startRow = 0, endRow = 50000, step=50000;
            for (int j = 0; j < 220; j++)
            {
                DataTable freebaseTopics = dbHandler.FillView("Type", @"SELECT TopicAutoId,TopicName 
                                                                    FROM Topic
                                                                    where TopicAutoId between "
                                                            + startRow + " and " + endRow
                                                            );
                StringBuilder csv = new StringBuilder();
                //Adding header row
                var headerLine = string.Format("{0},{1}{2}", "TopicID", "TopicName", Environment.NewLine);
                csv.Append(headerLine);
                for (int i = 0; i < freebaseTopics.Rows.Count; i++)
                {
                    string topicID = freebaseTopics.Rows[i]["TopicAutoId"].ToString();
                    string topicName = freebaseTopics.Rows[i]["TopicName"].ToString();

                    var newLine = string.Format("{0},{1}{2}", topicID, topicName, Environment.NewLine);
                    csv.Append(newLine);
                }
                File.WriteAllText(@"F:\Nodes\"+(j+1)+".csv", csv.ToString());
                Console.WriteLine("File No. " + (j + 1)+" written.");
                startRow = endRow + 1;
                endRow = endRow + step;
            }
        }
        static void Main(string[] args)
        {

            long startRow = 0, endRow = 0;
            InitNeo4JConnection();
            Console.WriteLine("Please enter start row:");
            startRow = long.Parse(Console.ReadLine());

            Console.WriteLine("Please enter end row:");
            endRow = long.Parse(Console.ReadLine());
            
            InsertTopics(startRow,endRow);
			InsertGraphRelationships();

            Console.WriteLine("Finished");
            Console.ReadLine();
   
        }
        public static void InitNeo4JConnection()
        {
           neo4jClient = new GraphClient(new Uri("http://localhost:7474/db/data"));
            neo4jClient.Connect();
            Console.WriteLine("Connection to Neo4J succeeded.");
        }
		
        #region Nodes & Links Insertion
        public static void InsertTopics(long startRow, long endRow)
        {
            Console.WriteLine("Creating graph nodes...");

            //Reading Freebase Topics from the relational database
            DataTable freebaseTopics = dbHandler.FillView("Type", @"SELECT TopicAutoId,TopicName 
                                                                    FROM Topic
                                                                    where TopicAutoId between "
                                                                    +startRow+ " and " +endRow 
                                                                    );
            for (int i = 0; i < freebaseTopics.Rows.Count; i++)
            {
                //Creating node of FreebaseType
                //Console.WriteLine("Creating Node " + i.ToString());
                try
                {

                    FreebaseTopic node = new FreebaseTopic();
                    node.ID = (long)freebaseTopics.Rows[i]["TopicAutoId"];

                    node.Name = freebaseTopics.Rows[i]["TopicName"].ToString();
                    CreateGraphNode(node);
                }
                catch
                {
                    Console.WriteLine("Problem at Topic ID:" + freebaseTopics.Rows[i]["TopicAutoId"].ToString());
                }
            }
        }
        static public void CreateGraphNode(FreebaseTopic node)
        {
            string nodeName = "type" + node.ID.ToString();
            neo4jClient.Cypher
                       .Create(" (" + nodeName + ":Topic {newNode}) ")
                       .WithParam("newNode", node)
                       .ExecuteWithoutResults();
        }

        static public void InsertGraphRelationships()
        {
            Console.WriteLine("Creating graph relationships...");
            //Reading Freebase Types relationships from the relational database
            DataTable typesRelationships = dbHandler.FillView("TypesNetwork", @"SELECT Source,Target
                                                                                FROM  TypesNetwork");
            for (int i = 0; i < typesRelationships.Rows.Count; i++)
            {
                Console.WriteLine("Creating Relationship " + i.ToString());
                //Creating node relationship 
                neo4jClient.Cypher
                .Match("(srcNode)", "(targetNode)")
                 .Where("srcNode.ID =" + typesRelationships.Rows[i]["Source"])
                .AndWhere("targetNode.ID=" + typesRelationships.Rows[i]["Target"])
                .Create("srcNode-[:Subclass_of]->targetNode")
                .ExecuteWithoutResults();
            }
        }
        #endregion
    }
}