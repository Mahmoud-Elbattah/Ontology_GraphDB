using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;
using System.Text.RegularExpressions;
namespace FreebaseTopicReader
{
    class Program
    {
        static void Main(string[] args)
        {
            DBInterface dbHandler = new DBInterface(".", "Freebase");

            StreamReader reader = new StreamReader(File.OpenRead(@"Topics_1.csv"));
            int topicAutoID = 1;
            int i = 1;
            while (!reader.EndOfStream)
            {
                string line = reader.ReadLine();
                line = line.Replace("'", "");
                line = line.Replace("-", "");
                line = line.Replace("*", "");
                line = line.Replace("%", "");
                line = line.Replace("$", "");
                line = line.Replace("!", "");
                line = line.Replace("?", "");
                line = line.Replace(">", "");
                line = line.Replace("<", "");
                string[] values = line.Replace("\"", "").Split(',');
                //Inserting topic into topic table
                if (values.Length > 0)
                {
                    values[0] = values[0].Replace(";", "");
                    values[0] = values[0].Replace(",", "");
                    dbHandler.ExecuteCommnad("Insert into Topic (TopicAutoId,TopicName) values(" + topicAutoID + ",'" + values[0] + "')");
                   
                    if (values.Length > 1)
                    {
                        string[] includedTypes = values[1].Split(';');
                        for (int j = 0; j < includedTypes.Length; j++)
                        {
                            try
                            {
                                DataTable targetType = dbHandler.FillView("Type", "Select TypeAutoID From Type Where TypeID='" + includedTypes[j] + "'");
                                Console.WriteLine("Topic no." + topicAutoID.ToString() + " inserted.");
                                if (targetType.Rows.Count > 0)
                                {
                                    string includedTypeId = targetType.Rows[0]["TypeAutoID"].ToString();
                                    dbHandler.ExecuteCommnad("Insert into TopicRelationships (TopicAutoID,SubClassOf) values(" + topicAutoID + "," + includedTypeId + ")");

                                }
                            }
                            catch
                            {
                                Console.WriteLine("Problem at record no." + i);
                            }

                        }

                    }
                    topicAutoID++;
                    i++;
                }


            }

            Console.WriteLine("Done!!");

        }
    }
}
