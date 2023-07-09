using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Loader
{
    public static class Loader
    {
        public static void Load(string conf)
        {
            using (SqlConnection connection = new SqlConnection(ParamsHelper.connectionStringData))
            {
                connection.Open();
                string fileMask = conf.Contains("ZTE_PM") ? "*ZTE_PM*.csv" : conf.Contains("ZTE_FM") ? "*ZTE_FM*.csv" : conf.Contains("ZTE_CM") ? "*ZTE_CM*.csv" : "";
                string[] files = Directory.GetFiles(ParamsHelper.Loader_inputFolder, fileMask);
                //int filecount = 0;
                foreach (string file in files)
                {
                    //while (filecount < 200)
                    //{
                    string tableName = GetTableNameFromFile(file, conf);
                    if (tableName != null)
                    {
                        DataTable dataTable = LoadDataFromFile(file);
                        if (dataTable != null)
                        {
                            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                            {
                                bulkCopy.DestinationTableName = tableName;
                                bulkCopy.BatchSize = 2000;

                                bulkCopy.WriteToServer(dataTable);
                            }
                            //Console.WriteLine($"Bulk insert completed for file: {file}");
                            string destinationFilePath = Path.Combine(ParamsHelper.Loader_outputFolder, Path.GetFileName(file));
                            File.Move(file, destinationFilePath);
                            //filecount++;
                        }
                    }
                }
            }
            //}

            Console.WriteLine("Bulk insert process finished. Press any key to exit.");
            Environment.Exit(0);
        }

        static string GetTableNameFromFile(string filePath, string conf)
        {
            string fileName = Path.GetFileNameWithoutExtension(filePath);

            if (conf.Contains("ZTE_FM_LOADER"))
            {
                return "TRANS_MW_ZTE_FM";
            }
            else if (conf.Contains("ZTE_CM_LOADER"))
            {
                if (fileName.Contains("_AIR_"))
                    return "TRANS_MW_ZTE_CM_AIR";
                else if (fileName.Contains("_AIRLINK"))
                    return "TRANS_MW_ZTE_CM_AIRLINK";
                else if (fileName.Contains("_BOARD"))
                    return "TRANS_MW_ZTE_CM_BOARD";
                else if (fileName.Contains("_ETHERNET"))
                    return "TRANS_MW_ZTE_CM_ETHERNET";
                else if (fileName.Contains("_MICROWAVE"))
                    return "TRANS_MW_ZTE_CM_MICROWAVE";
                else if (fileName.Contains("_NEINFO"))
                    return "TRANS_MW_ZTE_CM_NEINFO";
                else if (fileName.Contains("_PLA"))
                    return "TRANS_MW_ZTE_CM_PLA";
                else if (fileName.Contains("_TOPLINK"))
                    return "TRANS_MW_ZTE_CM_TOPLINK";
                else if (fileName.Contains("_TU"))
                    return "TRANS_MW_ZTE_CM_TU";
                else if (fileName.Contains("_XPIC"))
                    return "TRANS_MW_ZTE_CM_XPIC";
                else if (fileName.Contains("_IM_"))
                    return "TRANS_MW_ZTE_CM_INV";

                else return null;
            }
            else if (conf.Contains("ZTE_PM_LOADER"))
            {
                if (fileName.Contains("ACM_"))
                    return "TRANS_MW_ZTE_PM_ACM";
                else if (fileName.Contains("ENV_"))
                    return "TRANS_MW_ZTE_PM_ENV";
                else if (fileName.Contains("ODU_"))
                    return "TRANS_MW_ZTE_PM_ODU";
                else if (fileName.Contains("RMONQOS_"))
                    return "TRANS_MW_ZTE_PM_RMONQOS";
                else if (fileName.Contains("TRAFFICUNITRADIOLINKPERFORMANCE_"))
                    return "TRANS_MW_ZTE_PM_TRAFFICUNITRADIOLINKPERFORMANCE";
                else if (fileName.Contains("WE_"))
                    return "TRANS_MW_ZTE_PM_WE";
                else if (fileName.Contains("WETH_"))
                    return "TRANS_MW_ZTE_PM_WETH";
                else if (fileName.Contains("WL_"))
                    return "TRANS_MW_ZTE_PM_WL";
                else if (fileName.Contains("XPIC_"))
                    return "TRANS_MW_ZTE_PM_XPIC";

                else return null;
            }
            else
            {
                Console.WriteLine($"Skipping file: {filePath} - Unknown file type.");
                return null;
            }

        }
        static DataTable LoadDataFromFile(string filePath)
        {
            DataTable dataTable = new DataTable();

            using (TextFieldParser parser = new TextFieldParser(filePath))
            {
                parser.SetDelimiters(",");
                parser.HasFieldsEnclosedInQuotes = true;

                bool isColumnRow = true;
                int columnCount = 0;

                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();

                    if (isColumnRow)
                    {
                        isColumnRow = false;
                        columnCount = fields.Length;

                        foreach (string columnName in fields)
                        {
                            dataTable.Columns.Add(columnName);
                        }
                    }
                    else
                    {
                       // if (fields.Length == columnCount)
                       // {
                            dataTable.Rows.Add(fields);
                      //  }
                       // else
                       // {
                      //      Console.WriteLine($"Skipping invalid row in file: {filePath} - Number of values doesn't match the number of columns.");
                      //      Console.ReadKey();
                      //  }
                    }
                }
            }
            return dataTable;
        }
    }
}