using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mediator
{
    public class Program
    {
        static void Main(string[] args)
        {
            string Configuration = string.Empty;
            foreach (string arg in args)
            {
                Configuration = arg.ToString().Substring(arg.IndexOf("=") + 1).ToUpper();
            }
            ParamsHelper paramsHelper = new ParamsHelper();
            Mediator_ mediator_ = new Mediator_(paramsHelper,Configuration);
            mediator_.Start();
        }
    }
}

