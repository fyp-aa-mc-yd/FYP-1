using System;
using System.Configuration;
using System.Data.SqlClient;

namespace Loader
{
    public class ParamsHelper
    {
        public static string connectionString { get; private set; } = ConfigurationManager.ConnectionStrings["ParametersConnectionString"].ConnectionString;
        public static string connectionStringData { get; private set; } = ConfigurationManager.ConnectionStrings["FYPConnectionString"].ConnectionString;
        public static string Loader_inputFolder { get; private set; }
        public static string Loader_outputFolder { get; private set; }

        public static void GetParameters(string conf)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    string query = $"SELECT par_name, par_value FROM params where par_name like '%{conf}%'";
                    Console.WriteLine(query);
                    SqlCommand command = new SqlCommand(query, connection);

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string parValue = reader.GetString(1);
                            Loader_inputFolder = parValue;
                        }
                    }
                    Loader_outputFolder = GetOutputFolder(connection);
                }
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
            }
        }

        public static string GetOutputFolder(SqlConnection connection)
        {
            try
            {
                string query = $"SELECT par_name, par_value FROM params where par_name = 'loader_output_folder'";
                Console.WriteLine(query);
                SqlCommand command = new SqlCommand(query, connection);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string outputFolder = reader.GetString(1);
                        return outputFolder;
                    }
                }
                return string.Empty;
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                return string.Empty;
            }
        }
    }
}
