using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Mediator.Mediator_;

namespace Mediator
{
    public class TaskCreator
    {

        public static void CreateTask(List<TaskData> taskDataList, SqlConnection con)
        {
            string currentDateTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            DataTable dataTable = ConvertTaskDataListToDataTable(taskDataList);
            try
            {

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(con))
                {
                    bulkCopy.DestinationTableName = "tasks";
                    bulkCopy.WriteToServer(dataTable);
                }
            }
            catch (Exception ex)
            {
                if (IsDuplicateTask(ex))
                {
                    Console.WriteLine("Duplicate task found. Skipping insertion.");
                }
                else
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }
        private static DataTable ConvertTaskDataListToDataTable(List<TaskData> taskDataList)
        {
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("task_date", typeof(DateTime));
            dataTable.Columns.Add("file_name", typeof(string));
            dataTable.Columns.Add("file_path", typeof(string));
            dataTable.Columns.Add("task_status", typeof(int));

            foreach (TaskData taskData in taskDataList)
            {
                dataTable.Rows.Add(taskData.TaskDate, taskData.FileName, taskData.FilePath, taskData.TaskStatus);
            }

            return dataTable;
        }

        private static bool IsDuplicateTask(Exception ex)
        {
            if (ex is SqlException sqlException && sqlException.Number == 2601)
            {
                // Duplicate key error, handle it as a duplicate task
                return true;
            }
            return false;
        }
    }
}
