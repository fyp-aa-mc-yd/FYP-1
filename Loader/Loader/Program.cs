using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.Configuration;

namespace Loader
{
    public class Program
    {
        static void Main(string[] args)
        {
            //string Configuration = string.Empty;
            //foreach (string arg in args)
            //{
            //    Configuration = arg.ToString().Substring(arg.IndexOf("=") + 1).ToUpper();
            //    ParamsHelper.GetParameters(Configuration);
            //}
            //Loader.Load(Configuration);
            ParamsHelper.GetParameters("ZTE_CM_LOADER");
            Loader.Load("ZTE_CM_LOADER");
        }
    }
}   
