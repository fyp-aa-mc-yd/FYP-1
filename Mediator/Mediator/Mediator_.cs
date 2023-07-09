using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Configuration;
using System.Collections.Concurrent;
using System.Net;
using System.Threading;

namespace Mediator
{
    public class Mediator_
    {
        private ParamsHelper _paramsHelper = new ParamsHelper();
        string connectionString = ConfigurationManager.ConnectionStrings["FYPConnectionString"].ConnectionString;
        string _configuration;

        public Mediator_(ParamsHelper paramsHelper, string configuartion)
        {
            _paramsHelper = paramsHelper;
            _configuration = configuartion;
        }


        private static readonly string[] PM_mask = { "ACM_", "ENV_", "ODU_", "RMONQOS_", "TRAFFICUNITRADIOLINKPERFORMANCE_", "WE_", "WETH_",
                "WL_", "AXPIC_" };
        private static readonly string[] CM_mask = { "_AIR_", "_AIRLINK_", "_BOARD_", "_ETHERNET_", "_MICROWAVE_", "_NEINFO_", "_PLA_",
                "_TOPLINK_","_TU_", "_XPIC_","_IM_" };

        private static readonly string FM_mask = "10.2.156.54_";



        private void getParams()
        {
            ParamsHelper.GetParameters(_configuration);
        }

        public void RenameFiles()
        {
            string[] files = Directory.GetFiles(ParamsHelper.Mediator_inputFolder);

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                ConcurrentQueue<TaskData> taskDataQueue = new ConcurrentQueue<TaskData>();
                conn.Open();
                //int processedFiles = 0;
                Parallel.ForEach(files, (filePath) =>
                {
                    //if (Interlocked.Increment(ref processedFiles) > 200)
                    //{
                    //    state.Break();
                    //    return;
                    //}

                    string fileName = Path.GetFileName(filePath);
                    string newFileName = string.Empty;
                    int filesCount = files.Length;
                    string newPath = "";

                    if (ContainsAnyWord(fileName, PM_mask))
                        newFileName = "TRANS_MW_ZTE_PM_" + fileName;
                    else if (ContainsAnyWord(fileName, CM_mask))
                        newFileName = "TRANS_MW_ZTE_CM_" + fileName;
                    else if (fileName.Contains(FM_mask))
                        newFileName = "TRANS_MW_ZTE_FM_" + fileName;

                    if(newFileName!=string.Empty)
                        newPath = Path.Combine(ParamsHelper.Mediator_outputFolder, newFileName);
                    if (!File.Exists(newPath)&& newPath!="")
                    {
                        taskDataQueue.Enqueue(new TaskData
                        {
                            TaskDate = DateTime.Now,
                            FileName = newFileName,
                            FilePath = newPath,
                            TaskStatus = 1
                        });
                        File.Copy(filePath, newPath);
                    }
                });

                if (taskDataQueue.Count > 0)
                {
                    TaskCreator.CreateTask(taskDataQueue.ToList(), conn);
                }
            }
        }

        public class TaskData
        {
            public DateTime TaskDate { get; set; }
            public string FileName { get; set; }
            public string FilePath { get; set; }
            public int TaskStatus { get; set; }
        }

        public static bool ContainsAnyWord(string input, string[] words)
        {
            foreach (string word in words)
            {
                if (input.Contains(word))
                    return true;
            }
            return false;
        }
        public void Start()
        {
            getParams();
            RenameFiles();
        }
    }
}
